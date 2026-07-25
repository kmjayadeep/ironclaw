//! Baseline base-URL validation for this provider's outbound embedding calls.
//!
//! Adapted from `ironclaw_memory_mem0::url_check` (itself mirroring
//! `ironclaw_embeddings::url_check`) so every config-driven outbound provider
//! applies the same defense-in-depth SSRF gate. Baseline check, not the full
//! operator SSRF policy.
//!
//! Enforced here:
//! - URL parses
//! - Scheme is `http` or `https`
//! - No embedded userinfo (credentials belong in the redacted API key)
//! - Host present
//! - A literal-IP host is not `AlwaysBlocked`: cloud-metadata
//!   (`169.254.169.254`), link-local, multicast, unspecified `0.0.0.0`/`::`
//!
//! Deliberately NOT enforced:
//! - DNS resolution of hostnames
//! - Rejecting private/loopback IPs — a self-hosted embedding endpoint on
//!   localhost or a LAN address is legitimate
//!
//! Unlike the mem0 gate there is no vendor-cloud rule: any OpenAI-compatible
//! embedding endpoint is a valid target for this provider.

use std::net::{IpAddr, Ipv4Addr};

use crate::error::AmaAgentError;

/// Validate a configured outbound base URL.
///
/// No rejection carries the raw URL — only a redacted `reason` — so a
/// misconfigured host or a query-string token cannot leak into host logs.
pub(crate) fn check_base_url(url: &str) -> Result<(), AmaAgentError> {
    let parsed = reqwest::Url::parse(url).map_err(|error| AmaAgentError::InvalidUrl {
        reason: error.to_string(),
    })?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(AmaAgentError::InvalidUrl {
            reason: format!("only http/https are allowed (got '{scheme}')"),
        });
    }

    // Credentials in the URL would leak into logs and error text; they belong in
    // the redacted API key. The error deliberately echoes no part of the URL.
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AmaAgentError::InvalidUrl {
            reason: "must not embed credentials in the base URL (userinfo is not allowed)"
                .to_string(),
        });
    }

    let host = parsed.host_str().ok_or_else(|| AmaAgentError::InvalidUrl {
        reason: "missing host".to_string(),
    })?;

    // Strip IPv6 brackets before parsing as a literal IP.
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = normalized_host.parse::<IpAddr>()
        && is_always_blocked(&ip)
    {
        return Err(AmaAgentError::InvalidUrl {
            reason: format!("host '{host}' is not a permitted endpoint"),
        });
    }

    Ok(())
}

/// Addresses that are never a legitimate operator endpoint, regardless of policy.
fn is_always_blocked(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_link_local()
                || *v4 == Ipv4Addr::new(169, 254, 169, 254)
        }
        IpAddr::V6(v6) => {
            // An IPv4-mapped IPv6 address must not bypass the v4 rules.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_always_blocked(&IpAddr::V4(v4));
            }
            v6.is_unspecified() || v6.octets()[0] == 0xff || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normal_and_self_hosted_endpoints() {
        check_base_url("https://openrouter.ai/api/v1").unwrap();
        check_base_url("https://api.openai.com/v1").unwrap();
        // Self-hosted targets are legitimate at this layer.
        check_base_url("http://localhost:8080/v1").unwrap();
        check_base_url("http://127.0.0.1:8888/v1").unwrap();
        check_base_url("http://192.168.1.50:8000/v1").unwrap();
    }

    #[test]
    fn rejects_bad_scheme_and_missing_host() {
        assert!(check_base_url("file:///etc/passwd").is_err());
        assert!(check_base_url("ftp://example.com").is_err());
        assert!(check_base_url("not a url").is_err());
    }

    #[test]
    fn rejects_embedded_credentials_without_echoing_them() {
        let err = check_base_url("https://operator:s3cr3t-token@example.com/v1")
            .expect_err("userinfo must be rejected");
        let rendered = err.to_string();
        assert!(
            !rendered.contains("s3cr3t-token"),
            "the secret must never appear in the error: {rendered}"
        );
    }

    #[test]
    fn rejects_always_blocked_literal_ips() {
        // Cloud metadata — the classic SSRF target.
        assert!(check_base_url("http://169.254.169.254/v1").is_err());
        // Link-local, multicast, unspecified.
        assert!(check_base_url("http://169.254.1.1/v1").is_err());
        assert!(check_base_url("http://224.0.0.1/v1").is_err());
        assert!(check_base_url("http://0.0.0.0/v1").is_err());
        // IPv4-mapped IPv6 must not sneak metadata past the v4 rules.
        assert!(check_base_url("http://[::ffff:169.254.169.254]/v1").is_err());
        assert!(check_base_url("http://[::]/v1").is_err());
    }
}
