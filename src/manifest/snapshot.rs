use crate::ss_table_metadata::{SsTableLevel, SsTableMetadata};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

pub struct ManifestSnapshot {
    ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
}

impl ManifestSnapshot {
    pub(super) fn new(ss_tables: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>) -> Self {
        Self { ss_tables }
    }

    pub fn get_level(
        &self,
        level: SsTableLevel,
    ) -> Option<impl Iterator<Item = &Rc<SsTableMetadata>>> {
        match self.ss_tables.get(&level) {
            Some(set) => Some(set.iter()),
            None => None,
        }
    }
}
