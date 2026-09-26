use std::collections::BTreeSet;

pub type WalId = u64;

pub struct OrderedWalId {
    pub wal_id: WalId,
    pub order: u64,
}

pub struct WalIdGenerator {
    pool: BTreeSet<WalId>, // contains free ids. If some bug happens, we force only one WAL id to exists in the pool (set), so durability is preserved
    // (WAL won't be issued twice unless marking as free)
    next_ordered: u64, // sequential id to identify order in which WAL segments were created
}

impl WalIdGenerator {
    pub fn new(in_use_ids: &Vec<WalId>, next_order: u64, pool_size: u64) -> Self {
        let mut pool = BTreeSet::new();
        for id in 0..pool_size {
            pool.insert(id);
        }

        for id in in_use_ids {
            assert!(
                *id < pool_size,
                "Active WAL id cannot be greater than ID's pool size"
            );

            pool.remove(&id);
        }

        Self {
            pool,
            next_ordered: next_order,
        }
    }

    pub fn iter_free(&self) -> impl Iterator<Item = &WalId> {
        self.pool.iter()
    }

    pub fn mark_as_free(&mut self, wal_id: WalId) {
        self.pool.insert(wal_id);
    }

    pub fn next_id(&mut self) -> Option<OrderedWalId> {
        let wal_id = self.pool.pop_first();
        if let Some(id) = wal_id {
            let result = OrderedWalId {
                wal_id: id,
                order: self.next_ordered,
            };

            self.next_ordered += 1;
            return Some(result);
        }

        None
    }
}
