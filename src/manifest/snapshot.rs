use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

pub struct ManifestSnapshot {
    ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
    level_size_bytes: HashMap<SsTableLevel, u64>,
}

impl ManifestSnapshot {
    pub(super) fn new(
        ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
        level_size_bytes: HashMap<SsTableLevel, u64>,
    ) -> Self {
        Self {
            ss_tables,
            level_size_bytes,
        }
    }

    pub fn get_level(&self, level: SsTableLevel) -> Option<Rc<BTreeSet<Rc<SsTableMetadata>>>> {
        self.ss_tables.get(&level).cloned() // O(1), Rc clone
    }

    pub fn table_count_at_level(&self, level: SsTableLevel) -> usize {
        self.ss_tables.get(&level).map(|set| set.len()).unwrap_or(0)
    }

    pub fn levels_count(&self) -> usize {
        self.ss_tables.len()
    }

    pub fn get_level_size(&self, level: SsTableLevel) -> u64 {
        self.level_size_bytes.get(&level).cloned().unwrap_or(0)
    }
}
