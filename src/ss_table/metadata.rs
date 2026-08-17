use crate::db_entry::DbKey;
use std::cmp::Ordering;

pub type SsTableId = u32;
pub type SsTableLevel = u16;

#[derive(Default)]
pub struct SsTableMetadata {
    pub id: SsTableId,
    pub entry_count: u32,
    pub level: SsTableLevel,
    pub min_key: DbKey,
    pub max_key: DbKey,
}

impl Eq for SsTableMetadata {}

impl PartialEq<Self> for SsTableMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd<Self> for SsTableMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SsTableMetadata {
    fn cmp(&self, other: &Self) -> Ordering {
        self.min_key
            .cmp(&other.min_key)
            .then_with(|| self.max_key.cmp(&other.max_key))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl SsTableMetadata {
    pub fn min_key_bound(min_key: DbKey) -> Self {
        Self {
            min_key,
            ..Default::default()
        }
    }
}
