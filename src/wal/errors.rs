use crate::errors::retryable::{RetryableError, is_io_error_transient};

#[derive(Debug)]
pub enum WalWriterError {
    /// Could not open WAL file
    OpenFailed(std::io::Error),

    /// Could not write to WAL
    WriteFailed(std::io::Error),

    /// Could not complete fsync
    DurabilityError(std::io::Error),
}

impl RetryableError for WalWriterError {
    fn is_retryable(&self) -> bool {
        match self {
            WalWriterError::OpenFailed(e)
            | WalWriterError::WriteFailed(e)
            | WalWriterError::DurabilityError(e) => is_io_error_transient(e),
        }
    }
}

#[derive(Debug)]
pub enum WalError {
    OpenFailed(std::io::Error),
    CreateFailed(std::io::Error),
    WriterFailed(WalWriterError),
    ActorGone,
}

impl RetryableError for WalError {
    fn is_retryable(&self) -> bool {
        match self {
            WalError::OpenFailed(e) | WalError::CreateFailed(e) => is_io_error_transient(e),
            WalError::WriterFailed(e) => e.is_retryable(),
            WalError::ActorGone => false,
        }
    }
}
