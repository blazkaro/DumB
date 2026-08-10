use crate::entry::{DbEntry, DbKey, DbValue};

pub enum MemTableError {
    SizeExceeded,
}

pub trait MemTable {
    fn get(&self, key: &DbKey) -> Option<&DbValue>;
    fn set(&mut self, entry: DbEntry) -> Result<(), MemTableError>;
    fn remove(&mut self, key: DbKey);
    fn entry_count(&self) -> u32;
    fn iter(&self) -> impl Iterator<Item = (&DbKey, &DbValue)>;
    fn min_key(&self) -> &DbKey;
    fn max_key(&self) -> &DbKey;
}
