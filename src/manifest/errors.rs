use crate::errors::retryable::{RetryableError, is_io_error_transient};
use std::io;

#[derive(Debug)]
pub enum ManifestOpenError {
    Open(io::Error),
    Create(io::Error),
    Read(io::Error),
    DirMetadataSync(io::Error),
}

impl RetryableError for ManifestOpenError {
    fn is_retryable(&self) -> bool {
        match self {
            ManifestOpenError::Open(e)
            | ManifestOpenError::Create(e)
            | ManifestOpenError::Read(e)
            | ManifestOpenError::DirMetadataSync(e) => is_io_error_transient(e),
        }
    }
}

#[derive(Debug)]
pub enum ManifestWriteError {
    /// The write to disk failed. Nothing durable happened
    WriteFailed(io::Error),
    /// Write completed but fsync failed - durability is not guaranteed
    NotDurable(io::Error),
    /// Write was incomplete and wasn't applied
    IncompleteWrite,
    /// Manifest actor is no longer running
    ActorGone,
}

impl RetryableError for ManifestWriteError {
    fn is_retryable(&self) -> bool {
        match self {
            ManifestWriteError::WriteFailed(e) | ManifestWriteError::NotDurable(e) => {
                is_io_error_transient(e)
            }
            ManifestWriteError::IncompleteWrite => true,
            ManifestWriteError::ActorGone => false,
        }
    }
}
