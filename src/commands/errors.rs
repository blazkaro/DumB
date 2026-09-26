use crate::errors::retryable::RetryableError;
use crate::mem_table::errors::FlushError;
use crate::ss_table::errors::SsTableReadError;
use crate::wal::errors::{WalError, WalRotationError};
use std::rc::Rc;

#[derive(Debug)]
pub enum SetError {
    FlusherFailure(FlushError),
    WalFailure(Rc<WalError>),
    WalRotationFailure(WalRotationError),
}

impl RetryableError for SetError {
    fn is_retryable(&self) -> bool {
        match self {
            SetError::FlusherFailure(e) => e.is_retryable(),
            SetError::WalFailure(e) => e.is_retryable(),
            SetError::WalRotationFailure(e) => e.is_retryable(),
        }
    }
}

#[derive(Debug)]
pub enum GetError {
    /// Could not open ss table
    OpenFailure(std::io::Error),
    /// Could not read ss table
    ReadFailure(SsTableReadError),
}

pub type RemoveError = SetError;
