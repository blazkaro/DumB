use std::time::Duration;

pub enum WalMode {
    /// All commands happening inside batch_window are grouped, fsynced at once, then send in batch
    Strict {
        batch_window: Duration,
        max_batch_size: usize,
    },

    /// Commands are fsynced in batches, but response may be sent before ensuring durability (before group fsync happens)
    Relaxed {
        batch_window: Duration,
        max_batch_size: usize,
    },
}
