use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(u64);
static LAST_EVENT_ID: AtomicU64 = AtomicU64::new(0);
impl EventId {
    pub fn new() -> Self {
        Self(LAST_EVENT_ID.fetch_add(1, Relaxed))
    }
}
