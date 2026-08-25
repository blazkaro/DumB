use crate::db_entry::{DbKey, DbValue};
use crate::mem_table::mem_table::MemTable;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

/// Mem tables that have been rotated out of "active" but aren't yet
/// durably represented in the manifest. Shared between the rotation
/// path (pushes), the flusher (pops on success), and reads (scans).
#[derive(Clone)]
pub struct ImmutableMemTables<MT: MemTable> {
    inner: Rc<RefCell<VecDeque<Rc<MT>>>>,
}

impl<MT: MemTable> ImmutableMemTables<MT> {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    pub fn push(&self, mem_table: Rc<MT>) {
        self.inner.borrow_mut().push_back(mem_table);
    }

    pub fn pop(&self) {
        self.inner.borrow_mut().pop_front();
    }

    pub fn get(&self, key: &DbKey) -> Option<Rc<DbValue>> {
        // Reverse (from newest to oldest)
        for mem_table in self.inner.borrow().iter().rev() {
            if let Some(val) = mem_table.get(key) {
                return Some(val);
            }
        }

        None
    }
}
