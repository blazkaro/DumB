use crate::errors::retryable::{RetryableError, is_io_error_transient};
use crate::manifest::errors::ManifestWriteError;
use crate::ss_table::errors::{SsTableReadError, SsTableWriteError};

#[derive(Debug)]
pub enum CompactionTriggerError {
    CompactorGone,
}

impl RetryableError for CompactionTriggerError {
    fn is_retryable(&self) -> bool {
        match self {
            CompactionTriggerError::CompactorGone => false,
        }
    }
}

#[derive(Debug)]
pub enum MergeError {
    /// Could not open ss table
    OpenError(std::io::Error),
    /// Could not read ss table
    ReaderError(SsTableReadError),
    /// Could not create new ss table
    CreateError(std::io::Error),
    /// Could not write to ss table
    WriterError(SsTableWriteError),
    /// Could not sync directory metadata
    DirMetadataSync(std::io::Error),
    /// Could not persist in manifest
    ManifestRegistrationFailure(ManifestWriteError),
}

impl RetryableError for MergeError {
    fn is_retryable(&self) -> bool {
        match self {
            MergeError::OpenError(e)
            | MergeError::CreateError(e)
            | MergeError::DirMetadataSync(e) => is_io_error_transient(e),
            MergeError::ReaderError(e) => e.is_retryable(),
            MergeError::WriterError(e) => e.is_retryable(),
            MergeError::ManifestRegistrationFailure(e) => e.is_retryable(),
        }
    }
}

#[derive(Debug)]
pub enum CompactionError {
    /// Could not open ss tables directory
    DirOpenFailed(std::io::Error),
    // Merging ss tables failed
    MergeError(MergeError),
}

impl RetryableError for CompactionError {
    fn is_retryable(&self) -> bool {
        match self {
            CompactionError::MergeError(e) => e.is_retryable(),
            CompactionError::DirOpenFailed(e) => is_io_error_transient(e),
        }
    }
}
