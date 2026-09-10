use crate::commands::errors::{GetError, RemoveError, SetError};

#[derive(Debug)]
pub enum CommandRouterError {
    SetFailed(SetError),
    GetFailed(GetError),
    RemoveFailed(RemoveError),
}
