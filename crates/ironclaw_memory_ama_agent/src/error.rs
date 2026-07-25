//! Provider-local error type.
//!
//! Follows `ironclaw_memory_mem0::Mem0Error`'s conventions: variants carry only
//! a redacted `reason`/message, never a raw URL or request body, so a
//! misconfigured endpoint or an embedded token cannot leak into host logs.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AmaAgentError {
    /// The configured base URL failed the baseline SSRF/shape gate. Carries a
    /// redacted reason only — never the URL itself.
    #[error("invalid base url: {reason}")]
    InvalidUrl { reason: String },

    /// An outbound HTTP call failed (embedding endpoint). Message is the
    /// transport's own, which reqwest already redacts the URL from when built
    /// without `.url()` context.
    #[error("embedding transport failure: {0}")]
    Transport(String),

    /// The embedding endpoint answered, but not in the shape we require.
    #[error("embedding response malformed: {0}")]
    MalformedResponse(String),

    /// The LLM call underpinning extraction or the sufficiency judgment failed.
    /// Callers degrade rather than propagate — construction is best-effort and
    /// retrieval falls back to plain similarity.
    #[error("llm failure: {0}")]
    Llm(String),

    /// Reading or writing the persisted graph failed.
    #[error("graph storage failure: {0}")]
    Storage(String),

    /// An operation this provider deliberately does not implement (see the
    /// mapping-fidelity table in `lib.rs`).
    #[error("unsupported by the ama-agent memory provider: {0}")]
    Unsupported(&'static str),
}
