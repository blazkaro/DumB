use crate::commands::get::GetCommand;
use crate::commands::remove::RemoveCommand;
use crate::commands::set::SetCommand;
use std::io::Error;

pub enum CommandRequest {
    Set(SetCommand),       // 0
    Get(GetCommand),       // 1
    Remove(RemoveCommand), // 2
}

pub enum CommandDecodingError {
    Error(Error),
    InvalidInput,
}
