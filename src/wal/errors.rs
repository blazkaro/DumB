use crate::commands::errors::{RemoveError, SetError};
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
            WalWriterError::OpenFailed(e) | WalWriterError::WriteFailed(e) => {
                is_io_error_transient(e)
            }
            WalWriterError::DurabilityError(e) => false,
        }
    }
}

#[derive(Debug)]
pub enum WalReaderError {
    /// Could not open WAL file
    OpenFailed(std::io::Error),

    /// Could not read from WAL
    ReadFailed(std::io::Error),
}

impl RetryableError for WalReaderError {
    fn is_retryable(&self) -> bool {
        match self {
            WalReaderError::OpenFailed(e) | WalReaderError::ReadFailed(e) => {
                is_io_error_transient(e)
            }
        }
    }
}

#[derive(Debug)]
pub enum WalRecoveringError {
    SetError(SetError),
    RemoveError(RemoveError),
    OpenFailed(std::io::Error),
    ReaderFailed(WalReaderError),
    SegmentsListingFailed(std::io::Error),
}

impl RetryableError for WalRecoveringError {
    fn is_retryable(&self) -> bool {
        match self {
            WalRecoveringError::SetError(e) | WalRecoveringError::RemoveError(e) => {
                e.is_retryable()
            }
            WalRecoveringError::ReaderFailed(e) => e.is_retryable(),
            WalRecoveringError::OpenFailed(e) | WalRecoveringError::SegmentsListingFailed(e) => {
                is_io_error_transient(e)
            }
        }
    }
}

#[derive(Debug)]
pub enum WalRemoveError {
    RemoveFailed(std::io::Error),
}

impl RetryableError for WalRemoveError {
    fn is_retryable(&self) -> bool {
        match self {
            WalRemoveError::RemoveFailed(e) => is_io_error_transient(e),
        }
    }
}

#[derive(Debug)]
pub enum WalReleaseError {
    TruncationFailed(std::io::Error),
    DurabilityError(std::io::Error),
}

impl RetryableError for WalReleaseError {
    fn is_retryable(&self) -> bool {
        match self {
            WalReleaseError::TruncationFailed(e) => is_io_error_transient(e),
            WalReleaseError::DurabilityError(_) => false,
        }
    }
}

#[derive(Debug)]
pub enum WalRotationError {
    ExhaustedPool,
    ActorGone,
}

impl RetryableError for WalRotationError {
    fn is_retryable(&self) -> bool {
        match self {
            WalRotationError::ActorGone => false,
            WalRotationError::ExhaustedPool => true,
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
