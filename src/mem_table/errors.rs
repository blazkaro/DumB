use crate::db_entry::DbEntry;
use crate::errors::retryable::{RetryableError, is_io_error_transient};

#[derive(Debug)]
pub enum FlushError {
    /// Could not open ss tables directory
    DirOpenFailed(std::io::Error),
    /// Flusher task is no longer running
    FlusherGone,
}

impl RetryableError for FlushError {
    fn is_retryable(&self) -> bool {
        match self {
            FlushError::DirOpenFailed(e) => is_io_error_transient(e),
            FlushError::FlusherGone => false,
        }
    }
}

#[derive(Debug)]
pub enum MemTableSetError {
    SizeExceeded(DbEntry),
}

#[derive(Debug)]
pub enum MemTablePreallocationError{
    SizeExceeded
}
