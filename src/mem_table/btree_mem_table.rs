use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::mem_table::mem_table::{MemTable, MemTableError};
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct BTreeMemTable {
    map: BTreeMap<DbKey, Rc<DbValue>>,
    bytes_size: u32,
    storage_config: Rc<StorageConfig>,
}

impl MemTable for BTreeMemTable {
    fn new(storage_config: Rc<StorageConfig>) -> Self {
        Self {
            map: BTreeMap::new(),
            bytes_size: 0,
            storage_config,
        }
    }

    fn get(&self, key: &DbKey) -> Option<Rc<DbValue>> {
        self.map.get(key).cloned()
    }

    fn set(&mut self, entry: DbEntry) -> Result<(), MemTableError> {
        let new_entry_bytes_count = SsTableWriter::db_entry_bytes_size(&entry.key, &entry.value);
        let old = self.map.get(&entry.key); // avoid self.get - no refcount bump
        let mut delta: i32 = new_entry_bytes_count as i32;
        if let Some(value) = old {
            delta -= SsTableWriter::db_entry_bytes_size(&entry.key, value.as_ref()) as i32;
        }
        let _ = old;

        if delta <= 0 {
            self.bytes_size -= (-delta) as u32;
        } else {
            let delta = delta as u32;
            if self.bytes_size + delta > self.storage_config.memory_table_bytes_max_size {
                return Err(MemTableError::SizeExceeded(entry));
            }
            self.bytes_size += delta;
        }

        let _ = self.map.insert(entry.key, Rc::new(entry.value));
        Ok(())
    }

    fn entry_count(&self) -> u32 {
        self.map.len() as u32
    }

    fn iter(&self) -> impl Iterator<Item = (&DbKey, &Rc<DbValue>)> {
        self.map.iter()
    }

    fn min_key(&self) -> &DbKey {
        self.map.first_key_value().unwrap().0
    }

    fn max_key(&self) -> &DbKey {
        self.map.last_key_value().unwrap().0
    }
}
