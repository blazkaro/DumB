use crate::db_entry::{DbKey, DbValue};

pub struct SetCommand {
    pub key: DbKey,
    pub value: DbValue,
}
