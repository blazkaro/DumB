use crate::db_entry::DbKey;
use crate::manifest::snapshot::ManifestSnapshot;
use crate::ss_table::metadata::{SsTableLevel, SsTableMetadata};
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

pub struct SnapshotLookup {}

impl SnapshotLookup {
    pub fn get_overlap(
        target_level: SsTableLevel,
        min_key: DbKey,
        max_key: &DbKey,
        snapshot: &ManifestSnapshot,
    ) -> Vec<Rc<SsTableMetadata>> {
        let level = snapshot.get_level(target_level).unwrap_or_default();
        let lower_bound: SsTableMetadata = SsTableMetadata::min_key_bound(min_key.clone());

        let mut overlap = Vec::new();

        // Preceding (overlap may happen when table's min_key is lesser than our lower bound, but the lower bound is lesser than the table's max_key)
        if let Some(preceding) = level
            .range::<SsTableMetadata, _>((Unbounded, Excluded(&lower_bound)))
            .next_back()
        {
            if &preceding.max_key >= &min_key {
                overlap.push(Rc::clone(preceding));
            }
        }

        // For target levels > 0 ranges don't overlap, so min_key is sorted and take_while is safe.
        overlap.extend(
            level
                .range::<SsTableMetadata, _>((Included(&lower_bound), Unbounded))
                .take_while(|t| &t.min_key <= &max_key)
                .cloned(),
        );

        overlap
    }
}
