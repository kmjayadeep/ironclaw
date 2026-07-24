//! Attachment vocabulary shared by product adapters, workflow, and landing.
//!
//! These are the byte-carrying attachment DTOs referenced by product-adapter
//! contracts (e.g. [`crate::product_adapter::ChannelAdapter`]); the landing
//! and budget logic that consumes them lives in `ironclaw_attachments`.

use std::fmt;

use crate::ScopedPath;

/// One inbound attachment with its raw bytes, ready to be landed and turned
/// into a transcript attachment reference.
///
/// The attachment kind and the fallback filename extension are *derived from*
/// `mime_type` against the attachment format registry by the landing routine —
/// the authoritative source — so callers cannot drift them out of sync with
/// the MIME type they pass.
#[derive(Debug, Clone)]
pub struct InboundAttachment {
    /// Stable identifier for this attachment within its message.
    pub id: String,
    /// MIME type as received at the ingress boundary. The attachment kind and
    /// fallback extension are derived from this.
    pub mime_type: String,
    /// Original filename, when the source provided one.
    pub filename: Option<String>,
    /// Raw attachment bytes to land in the project filesystem.
    pub bytes: Vec<u8>,
}

/// A trusted, in-memory file whose path has already been validated by its
/// owning filesystem boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct MaterializedFile<P> {
    pub path: P,
    pub filename: Option<String>,
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

impl<P: fmt::Debug> fmt::Debug for MaterializedFile<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaterializedFile")
            .field("path", &self.path)
            .field("filename", &self.filename)
            .field("mime_type", &self.mime_type)
            .field("size_bytes", &self.bytes.len())
            .finish()
    }
}

impl<P> MaterializedFile<P> {
    pub fn size_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// A file from a thread's scoped project workspace.
pub type WorkspaceFile = MaterializedFile<ScopedPath>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_materialized_bytes() {
        let file = MaterializedFile {
            path: "/workspace/report.txt".to_string(),
            filename: Some("report.txt".to_string()),
            mime_type: "text/plain".to_string(),
            bytes: b"byte-sentinel-must-not-leak".to_vec(),
        };

        let rendered = format!("{file:?}");
        assert!(rendered.contains("/workspace/report.txt"));
        assert!(rendered.contains("size_bytes"));
        assert!(!rendered.contains("byte-sentinel-must-not-leak"));
        assert!(!rendered.contains("98, 121, 116, 101"));
    }
}
