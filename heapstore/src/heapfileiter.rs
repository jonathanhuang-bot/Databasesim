use crate::heapfile::HeapFile;
use crate::page::PageIntoIter;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    heap_file: Arc<HeapFile>,
    page_iter: Option<PageIntoIter>,
    tid: TransactionId,
    currentpid: u16,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        HeapFileIterator {
            heap_file: hf,
            page_iter: None,
            tid,
            currentpid: 0,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.page_iter.is_none() {
                let first_page = match self.heap_file.read_page_from_file(self.currentpid) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                self.page_iter = Some(first_page.into_iter());
            }
            if let Some(iter) = &mut self.page_iter {
                if let Some((value, slot_id)) = iter.next() {
                    return Some((value, ValueId::new(slot_id)));
                }
            }
            self.currentpid += 1;

            if self.currentpid >= self.heap_file.num_pages() {
                return None;
            }
            let page = match self.heap_file.read_page_from_file(self.currentpid) {
                Ok(p) => p,
                Err(_) => continue,
            };
            self.page_iter = Some(page.into_iter());
        }
    }
}
