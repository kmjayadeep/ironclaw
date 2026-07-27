//! A minimal OpenAI-compatible chat client exposed as an [`LlmProvider`].
//!
//! # Why the provider carries its own client
//!
//! This crate's two LLM call sites (extraction, sufficiency judgment) need a
//! model, but the composed runtime's model gateway lives in `ironclaw_operator`
//! (layer `products`), which a `substrates` crate may not depend on. The memory
//! provider is also constructed synchronously in the composition factory, before
//! any async provider factory has run. So instead of reaching for the agent's
//! gateway, the provider owns a small client of its own — the same shape as
//! [`crate::embedding::OpenAiCompatEmbedder`], sharing its SSRF gate.
//!
//! Deliberately NOT reused here: `ironclaw_llm`'s retry/backoff and cost
//! instrumentation. These are two short, best-effort calls per turn whose failure
//! already degrades gracefully (extraction → record the raw turn; judgment →
//! plain similarity retrieval), so a failed call costs retrieval quality rather
//! than correctness. Documented so nobody mistakes it for an oversight.

use async_trait::async_trait;
use ironclaw_llm::{
    CompletionRequest, CompletionResponse, FinishReason, LlmError, LlmProvider,
    ToolCompletionRequest, ToolCompletionResponse,
};
use rust_decimal::Decimal;

use crate::error::AmaAgentError;

/// Provider label carried on errors so a failure is attributable to memory
/// rather than to the benchmarked agent's own model calls.
const PROVIDER_LABEL: &str = "ama-agent-memory";

/// Chat completions against any OpenAI-compatible `/v1/chat/completions`.
pub struct OpenAiCompatChat {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    model: String,
}

impl OpenAiCompatChat {
    /// `base_url` should be the API root (e.g. `https://openrouter.ai/api/v1`);
    /// `/chat/completions` is appended.
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        model: impl Into<String>,
    ) -> Result<Self, AmaAgentError> {
        let base_url = base_url.into();
        crate::url_check::check_base_url(&base_url)?;
        let client = reqwest::Client::builder()
            // Bounded so a hung endpoint cannot stall a benchmark run. Generous
            // enough for an extraction call over a long observation.
            .timeout(std::time::Duration::from_secs(120))
            // An LLM endpoint has no legitimate reason to redirect, and following
            // one could send the key to another host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| AmaAgentError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            model: model.into(),
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatChat {
    fn model_name(&self) -> &str {
        &self.model
    }

    fn cost_per_token(&self) -> (Decimal, Decimal) {
        // This provider's calls are memory-internal overhead, not the benchmarked
        // agent's spend, and it has no pricing table. Report zero rather than
        // invent numbers that would pollute a run's cost accounting.
        (Decimal::ZERO, Decimal::ZERO)
    }

    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let messages: Vec<serde_json::Value> = request
            .messages
            .iter()
            .map(|m| {
                let role = match m.role {
                    ironclaw_llm::Role::System => "system",
                    ironclaw_llm::Role::User => "user",
                    ironclaw_llm::Role::Assistant => "assistant",
                    ironclaw_llm::Role::Tool => "tool",
                };
                serde_json::json!({ "role": role, "content": m.content })
            })
            .collect();

        let mut body = serde_json::json!({ "model": self.model, "messages": messages });
        if let Some(t) = request.temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(m) = request.max_tokens {
            body["max_tokens"] = serde_json::json!(m);
        }

        let mut http = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .json(&body);
        if let Some(key) = &self.api_key {
            http = http.bearer_auth(key);
        }
        let response = http.send().await.map_err(|e| LlmError::RequestFailed {
            provider: PROVIDER_LABEL.to_string(),
            reason: format!("chat transport: {e}"),
        })?;

        let status = response.status();
        if !status.is_success() {
            // The body is not echoed: an error body can quote the request, and the
            // request carries recorded memory text.
            return Err(LlmError::RequestFailed {
                provider: PROVIDER_LABEL.to_string(),
                reason: format!("chat endpoint returned HTTP {status}"),
            });
        }

        let parsed: serde_json::Value =
            response.json().await.map_err(|e| LlmError::RequestFailed {
                provider: PROVIDER_LABEL.to_string(),
                reason: format!("chat decode: {e}"),
            })?;

        let content = parsed
            .pointer("/choices/0/message/content")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let usage = |key: &str| -> u32 {
            parsed
                .pointer(&format!("/usage/{key}"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32
        };

        Ok(CompletionResponse {
            content,
            input_tokens: usage("prompt_tokens"),
            output_tokens: usage("completion_tokens"),
            finish_reason: FinishReason::Stop,
            reasoning: None,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        })
    }

    async fn complete_with_tools(
        &self,
        _request: ToolCompletionRequest,
    ) -> Result<ToolCompletionResponse, LlmError> {
        // Extraction and the sufficiency judgment are plain completions; this
        // provider never issues tool calls.
        Err(LlmError::RequestFailed {
            provider: PROVIDER_LABEL.to_string(),
            reason: "this provider does not use tool completions".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_bad_base_url_and_accepts_a_normal_one() {
        // Same SSRF gate as the embedder: credentials in the URL, non-http
        // schemes, and cloud-metadata hosts must all fail closed.
        assert!(OpenAiCompatChat::new("https://user:pw@example.com/v1", None, "m").is_err());
        assert!(OpenAiCompatChat::new("file:///etc/passwd", None, "m").is_err());
        assert!(OpenAiCompatChat::new("http://169.254.169.254/v1", None, "m").is_err());
        assert!(OpenAiCompatChat::new("https://openrouter.ai/api/v1", None, "m").is_ok());
    }

    #[test]
    fn reports_its_model_and_zero_cost() {
        let c = OpenAiCompatChat::new("https://openrouter.ai/api/v1", None, "some/model").unwrap();
        assert_eq!(c.model_name(), "some/model");
        // Memory-internal overhead must not be attributed to the agent's spend.
        assert_eq!(c.cost_per_token(), (Decimal::ZERO, Decimal::ZERO));
    }

    #[test]
    fn trailing_slash_in_base_url_does_not_double_the_path() {
        let c = OpenAiCompatChat::new("https://openrouter.ai/api/v1/", None, "m").unwrap();
        assert_eq!(c.base_url, "https://openrouter.ai/api/v1");
    }
}
