use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

pub struct ManifestSnapshot {
    ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
}

impl ManifestSnapshot {
    pub(super) fn new(ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>) -> Self {
        Self { ss_tables }
    }

    pub fn get_level(&self, level: SsTableLevel) -> Option<Rc<BTreeSet<Rc<SsTableMetadata>>>> {
        self.ss_tables.get(&level).cloned() // O(1), Rc clone
    }

    pub fn table_count_at_level(&self, level: SsTableLevel) -> usize {
        self.ss_tables.get(&level).map(|set| set.len()).unwrap_or(0)
    }
}
