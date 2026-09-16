use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct SocketTable {
    entries: BTreeMap<u64, SocketPlaceholder>,
}

#[derive(Debug)]
struct SocketPlaceholder;

impl SocketTable {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
