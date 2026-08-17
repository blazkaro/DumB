use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::mem_table::mem_table::{MemTable, MemTableError};
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct BTreeMemTable {
    map: BTreeMap<DbKey, DbValue>,
    bytes_size: u32,
    storage_config: Rc<StorageConfig>,
}

impl BTreeMemTable {
    pub fn new(storage_config: Rc<StorageConfig>) -> Self {
        Self {
            map: BTreeMap::new(),
            bytes_size: 0,
            storage_config,
        }
    }
}

impl MemTable for BTreeMemTable {
    fn get(&self, key: &DbKey) -> Option<&DbValue> {
        self.map.get(key)
    }

    fn set(&mut self, entry: DbEntry) -> Result<(), MemTableError> {
        let entry_bytes_count = SsTableWriter::db_entry_bytes_size(&entry);
        if self.bytes_size + entry_bytes_count > self.storage_config.memory_table_bytes_max_size {
            return Err(MemTableError::SizeExceeded);
        }

        self.bytes_size += entry_bytes_count;
        self.map.insert(entry.key, entry.value);
        Ok(())
    }

    fn remove(&mut self, key: DbKey) {
        let old = self.map.insert(key, DbValue::Tombstone);
        if let Some(value) = old {
            // estimated size change
            match &value {
                DbValue::Value(bytes) => {
                    self.bytes_size -= size_of::<u32>() as u32; // decrease by value len
                    self.bytes_size -= bytes.len() as u32; // decrease by value bytes
                }
                _ => return,
            }
        }
    }

    fn entry_count(&self) -> u32 {
        self.map.len() as u32
    }

    fn iter(&self) -> impl Iterator<Item = (&DbKey, &DbValue)> {
        self.map.iter()
    }

    fn min_key(&self) -> &DbKey {
        self.map.first_key_value().unwrap().0
    }

    fn max_key(&self) -> &DbKey {
        self.map.last_key_value().unwrap().0
    }
}
