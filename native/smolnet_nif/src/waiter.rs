use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct ReadyQueue {
    entries: VecDeque<ReadyPlaceholder>,
}

#[derive(Debug)]
struct ReadyPlaceholder;

impl ReadyQueue {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
