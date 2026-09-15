use std::rc::Rc;
use crate::mem_table::errors::FlushError;
use crate::ss_table::errors::SsTableReadError;
use crate::wal::errors::WalError;

#[derive(Debug)]
pub enum SetError {
    FlusherFailure(FlushError),
    WalFailure(Rc<WalError>),
}

#[derive(Debug)]
pub enum GetError {
    /// Could not open ss table
    OpenFailure(std::io::Error),
    /// Could not read ss table
    ReadFailure(SsTableReadError),
}

pub type RemoveError = SetError;
