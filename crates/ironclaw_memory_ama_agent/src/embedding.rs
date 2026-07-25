//! Embedding access for similarity retrieval.
//!
//! A crate-local trait rather than a dependency on `ironclaw_embeddings`:
//! that crate is v1-only with no `crates/*` dependents, and Reborn's
//! host-mediated embedding port (`memory.semantic_search.v1`) is explicitly
//! deferred upstream. `ironclaw_memory_native` sets the same precedent with its
//! own small `EmbeddingProvider` trait.
//!
//! Two implementations ship:
//! - [`OpenAiCompatEmbedder`] — the real one, any OpenAI-compatible
//!   `/v1/embeddings` endpoint (verified against OpenRouter).
//! - [`HashEmbedder`] — deterministic, offline, `test-support` only. Emphatically
//!   NOT a semantic embedder; it exists so construction/retrieval wiring can be
//!   tested without a network. Benchmark numbers must come from the real one.

use async_trait::async_trait;

use crate::error::AmaAgentError;

/// Produces embedding vectors for node/query text.
#[async_trait]
pub trait AmaEmbeddingProvider: Send + Sync {
    /// Vector width. Callers use it only for sanity checks; cosine similarity
    /// already tolerates a mismatch by returning 0.0.
    fn dimension(&self) -> usize;

    /// Embed a batch. Implementations must return exactly one vector per input,
    /// in order, or `Err` — a short/reordered result would silently mis-associate
    /// vectors with nodes.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AmaAgentError>;

    /// Convenience single embed, used for the query side of retrieval.
    async fn embed(&self, text: &str) -> Result<Vec<f32>, AmaAgentError> {
        let mut out = self.embed_batch(&[text.to_string()]).await?;
        out.pop()
            .ok_or_else(|| AmaAgentError::MalformedResponse("empty embedding batch".into()))
    }
}

/// Real embedder against any OpenAI-compatible `/v1/embeddings` endpoint.
pub struct OpenAiCompatEmbedder {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    model: String,
    dimension: usize,
    /// Per-request input cap. The endpoint's own token limit is what actually
    /// matters (e.g. text-embedding-3-small caps at 8191 tokens); truncating by
    /// characters is a cheap guard that keeps a pathological observation from
    /// hard-failing the whole batch.
    max_chars: usize,
}

impl OpenAiCompatEmbedder {
    /// `base_url` should be the API root (e.g. `https://openrouter.ai/api/v1`);
    /// `/embeddings` is appended.
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        model: impl Into<String>,
        dimension: usize,
    ) -> Result<Self, AmaAgentError> {
        let base_url = base_url.into();
        crate::url_check::check_base_url(&base_url)?;
        let client = reqwest::Client::builder()
            // Bounded so a hung endpoint cannot stall a whole benchmark run.
            .timeout(std::time::Duration::from_secs(60))
            // No redirects: an embedding endpoint has no legitimate reason to
            // redirect, and following one could send the key to another host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| AmaAgentError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            model: model.into(),
            dimension,
            max_chars: 8_000,
        })
    }
}

#[async_trait]
impl AmaEmbeddingProvider for OpenAiCompatEmbedder {
    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AmaAgentError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let truncated: Vec<String> = texts
            .iter()
            .map(|t| {
                if t.len() <= self.max_chars {
                    t.clone()
                } else {
                    // Truncate on a char boundary — slicing bytes would panic on
                    // multi-byte content, which real observations contain.
                    t.chars().take(self.max_chars).collect()
                }
            })
            .collect();

        let mut request = self
            .client
            .post(format!("{}/embeddings", self.base_url))
            .json(&serde_json::json!({ "model": self.model, "input": truncated }));
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(key);
        }
        let response = request
            .send()
            .await
            .map_err(|e| AmaAgentError::Transport(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            // Body deliberately not included: an error body can echo the request,
            // and the request carries the memory text.
            return Err(AmaAgentError::Transport(format!(
                "embedding endpoint returned HTTP {status}"
            )));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| AmaAgentError::MalformedResponse(e.to_string()))?;
        let data = body
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| AmaAgentError::MalformedResponse("missing `data` array".into()))?;
        if data.len() != truncated.len() {
            return Err(AmaAgentError::MalformedResponse(format!(
                "expected {} embeddings, got {}",
                truncated.len(),
                data.len()
            )));
        }

        let mut out = Vec::with_capacity(data.len());
        for item in data {
            let vector = item
                .get("embedding")
                .and_then(|e| e.as_array())
                .ok_or_else(|| AmaAgentError::MalformedResponse("missing `embedding`".into()))?
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect::<Vec<f32>>();
            if vector.is_empty() {
                return Err(AmaAgentError::MalformedResponse(
                    "empty embedding vector".into(),
                ));
            }
            out.push(vector);
        }
        Ok(out)
    }
}

/// Deterministic offline embedder for tests.
///
/// Hashes tokens into a fixed-width bag-of-words vector, L2-normalized. It gives
/// identical text identical vectors and different text different vectors, which
/// is all the wiring tests need — but it carries NO semantic generalization, so
/// it must never back a reported benchmark number.
#[cfg(any(test, feature = "test-support"))]
pub struct HashEmbedder {
    dimension: usize,
}

#[cfg(any(test, feature = "test-support"))]
impl HashEmbedder {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension: dimension.max(8),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl AmaEmbeddingProvider for HashEmbedder {
    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AmaAgentError> {
        Ok(texts
            .iter()
            .map(|t| {
                let mut v = vec![0.0f32; self.dimension];
                for token in crate::graph::normalize(t).split_whitespace() {
                    let idx = (crate::graph::NodeId::of_text(token).0 as usize) % self.dimension;
                    v[idx] += 1.0;
                }
                let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for x in &mut v {
                        *x /= norm;
                    }
                }
                v
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hash_embedder_is_deterministic_and_discriminating() {
        let e = HashEmbedder::new(64);
        let a = e.embed("the chest is unlocked").await.unwrap();
        let a2 = e.embed("The  Chest  Is  Unlocked").await.unwrap();
        let b = e.embed("a totally different fact entirely").await.unwrap();

        assert_eq!(a, a2, "normalization makes identical text identical");
        assert_ne!(a, b, "different text must differ");
        assert_eq!(a.len(), 64);

        // Unit length, so cosine similarity behaves.
        let norm: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "expected L2-normalized, got {norm}"
        );

        // Self-similarity 1, cross-similarity lower — the property retrieval needs.
        assert!(crate::graph::cosine_similarity(&a, &a2) > 0.99);
        assert!(crate::graph::cosine_similarity(&a, &b) < 0.99);
    }

    #[tokio::test]
    async fn embed_batch_preserves_order_and_handles_empty() {
        let e = HashEmbedder::new(32);
        assert!(e.embed_batch(&[]).await.unwrap().is_empty());

        let texts = vec!["alpha".to_string(), "beta".to_string(), "alpha".to_string()];
        let out = e.embed_batch(&texts).await.unwrap();
        assert_eq!(out.len(), 3, "one vector per input");
        assert_eq!(out[0], out[2], "same input => same vector, positionally");
        assert_ne!(out[0], out[1]);
    }

    #[test]
    fn real_embedder_rejects_a_bad_base_url() {
        // Credentials in the URL and non-http schemes must fail closed rather
        // than be silently accepted and leaked into logs.
        assert!(OpenAiCompatEmbedder::new("https://user:pw@example.com/v1", None, "m", 8).is_err());
        assert!(OpenAiCompatEmbedder::new("file:///etc/passwd", None, "m", 8).is_err());
        // Cloud metadata endpoint is always blocked.
        assert!(OpenAiCompatEmbedder::new("http://169.254.169.254/v1", None, "m", 8).is_err());
        // A normal endpoint is accepted.
        assert!(OpenAiCompatEmbedder::new("https://openrouter.ai/api/v1", None, "m", 8).is_ok());
    }
}
