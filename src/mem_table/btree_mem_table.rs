use crate::db_entry::{DbEntry, DbKey, DbValue};
use crate::mem_table::errors::{MemTablePreallocationError, MemTableSetError};
use crate::mem_table::mem_table::MemTable;
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use std::collections::BTreeMap;
use std::rc::Rc;

#[derive(Debug)]
pub struct BTreeMemTable {
    map: BTreeMap<DbKey, Rc<DbValue>>,
    bytes_size: u32,
    storage_config: Rc<StorageConfig>,
}

impl BTreeMemTable {
    fn get_delta(&self, entry: &DbEntry) -> i64 {
        let new_entry_bytes_count = SsTableWriter::db_entry_bytes_size(&entry.key, &entry.value);
        let old = self.map.get(&entry.key); // avoid self.get - no refcount bump
        let mut delta: i64 = new_entry_bytes_count as i64;
        if let Some(value) = old {
            delta -= SsTableWriter::db_entry_bytes_size(&entry.key, value.as_ref()) as i64;
        }

        delta
    }
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

    fn set(&mut self, entry: DbEntry, pre_allocated: bool) -> Result<(), MemTableSetError> {
        if !pre_allocated {
            let delta = self.get_delta(&entry);
            if delta > 0
                && self.bytes_size + delta as u32 > self.storage_config.memory_table_bytes_max_size
            {
                return Err(MemTableSetError::SizeExceeded(entry));
            }

            if delta <= 0 {
                self.bytes_size -= (-delta) as u32;
            } else {
                self.bytes_size += delta as u32;
            }
        }

        let _ = self.map.insert(entry.key, Rc::new(entry.value));
        Ok(())
    }

    fn preallocate(&mut self, entry: &DbEntry) -> Result<(), MemTablePreallocationError> {
        let delta = self.get_delta(entry);
        if delta > 0
            && self.bytes_size + delta as u32 > self.storage_config.memory_table_bytes_max_size
        {
            return Err(MemTablePreallocationError::SizeExceeded);
        }

        if delta <= 0 {
            self.bytes_size -= (-delta) as u32;
        } else {
            self.bytes_size += delta as u32;
        }

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
