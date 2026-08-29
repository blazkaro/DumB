use crate::ss_table::metadata::SsTableId;
use std::cell::Cell;

pub struct SsTableIdGenerator {
    next_id: Cell<SsTableId>,
}

impl SsTableIdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: Cell::new(0), // TODO: read from disk
        }
    }

    pub fn next_id(&self) -> SsTableId {
        let next_free = self.next_id.get();
        self.next_id.set(next_free + 1);
        next_free
    }
}
