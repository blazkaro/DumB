use crate::entry::DbKey;

pub type SsTableId = u32;
pub type SsTableLevel = u16;

pub struct SsTableMetadata {
    pub id: SsTableId,
    pub entry_count: u32,
    pub level: SsTableLevel,
    pub min_key: DbKey,
    pub max_key: DbKey,
}
