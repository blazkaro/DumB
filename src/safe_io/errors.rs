use crate::errors::retryable::{RetryableError, is_io_error_transient};

#[derive(Debug)]
pub enum SafeIoInitError {
    OpenFailed(std::io::Error),
}

impl RetryableError for SafeIoInitError {
    fn is_retryable(&self) -> bool {
        match self {
            SafeIoInitError::OpenFailed(e) => is_io_error_transient(e),
        }
    }
}

#[derive(Debug)]
pub enum SafeWriteError {
    IoError(std::io::Error),
}

impl RetryableError for SafeWriteError {
    fn is_retryable(&self) -> bool {
        match self {
            SafeWriteError::IoError(_) => false,
        }
    }
}

#[derive(Debug)]
pub enum SafeReadError {
    CorruptedEntry,
    TruncatedEntry,
    IoError(std::io::Error),
}

impl RetryableError for SafeReadError {
    fn is_retryable(&self) -> bool {
        match self {
            SafeReadError::TruncatedEntry
            | SafeReadError::CorruptedEntry
            | SafeReadError::IoError(_) => false,
        }
    }
}
