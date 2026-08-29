use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::storage_config::StorageConfig;
use std::rc::Rc;

pub enum MemTableError {
    SizeExceeded(DbEntry),
}

pub trait MemTable {
    fn new(storage_config: Rc<StorageConfig>) -> Self;
    fn get(&self, key: &DbKey) -> Option<Rc<DbValue>>;
    fn set(&mut self, entry: DbEntry) -> Result<(), MemTableError>;
    fn entry_count(&self) -> u32;
    fn iter(&self) -> impl Iterator<Item = (&DbKey, &Rc<DbValue>)>;
    fn min_key(&self) -> &DbKey;
    fn max_key(&self) -> &DbKey;
}
