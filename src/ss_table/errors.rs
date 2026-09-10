use crate::errors::retryable::{RetryableError, is_io_error_transient};

#[derive(Debug)]
pub enum SsTableWriteError {
    InitFailed(std::io::Error),
    WriteFailed(std::io::Error),
    DurabilityError(std::io::Error),
}

impl RetryableError for SsTableWriteError {
    fn is_retryable(&self) -> bool {
        match self {
            SsTableWriteError::InitFailed(e)
            | SsTableWriteError::WriteFailed(e)
            | SsTableWriteError::DurabilityError(e) => is_io_error_transient(e),
        }
    }
}

#[derive(Debug)]
pub enum SsTableReadError {
    ReadFailed(std::io::Error),
    CloseFailed(std::io::Error),
}

impl RetryableError for SsTableReadError {
    fn is_retryable(&self) -> bool {
        match self {
            SsTableReadError::ReadFailed(e) | SsTableReadError::CloseFailed(e) => {
                is_io_error_transient(e)
            }
        }
    }
}
