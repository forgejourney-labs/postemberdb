//! Reusable page-ID allocation primitives.
//!
//! This allocator is deliberately independent from the snapshot codec. It is
//! not persisted yet; durable allocator metadata will be introduced together
//! with the page-file format that consumes it.

use super::DbError;
use std::collections::BTreeSet;

/// Tracks allocated page IDs and reuses released IDs before extending the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PageAllocator {
    next_page_id: u32,
    free_pages: BTreeSet<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AllocatorStats {
    /// Number of page IDs ever handed out, including currently free IDs.
    pub high_water_mark: u32,
    pub free_pages: usize,
}

impl PageAllocator {
    /// Treats all page IDs below `existing_pages` as allocated.
    pub(super) fn with_existing_pages(existing_pages: u32) -> Self {
        Self {
            next_page_id: existing_pages,
            free_pages: BTreeSet::new(),
        }
    }

    /// Returns the lowest reusable page ID, or extends the high-water mark.
    pub(super) fn allocate(&mut self) -> Result<u32, DbError> {
        if let Some(page_id) = self.free_pages.iter().next().copied() {
            self.free_pages.remove(&page_id);
            return Ok(page_id);
        }

        let page_id = self.next_page_id;
        self.next_page_id = self.next_page_id.checked_add(1)
            .ok_or_else(|| DbError::Storage("page ID space exhausted".into()))?;
        Ok(page_id)
    }

    /// Releases an allocated page ID. Unknown IDs and double frees are errors.
    pub(super) fn release(&mut self, page_id: u32) -> Result<(), DbError> {
        if page_id >= self.next_page_id {
            return Err(DbError::Storage(format!("cannot release unallocated page {page_id}")));
        }
        if !self.free_pages.insert(page_id) {
            return Err(DbError::Storage(format!("page {page_id} is already free")));
        }
        Ok(())
    }

    pub(super) fn is_allocated(&self, page_id: u32) -> bool {
        page_id < self.next_page_id && !self.free_pages.contains(&page_id)
    }

    pub(super) fn stats(&self) -> AllocatorStats {
        AllocatorStats {
            high_water_mark: self.next_page_id,
            free_pages: self.free_pages.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_released_page_before_growing_file() {
        let mut allocator = PageAllocator::with_existing_pages(3);
        allocator.release(1).unwrap();

        assert_eq!(allocator.allocate().unwrap(), 1);
        assert_eq!(allocator.allocate().unwrap(), 3);
        assert_eq!(allocator.stats().high_water_mark, 4);
    }

    #[test]
    fn reuses_lowest_page_id_and_coalesces_free_space_implicitly() {
        let mut allocator = PageAllocator::with_existing_pages(5);
        allocator.release(3).unwrap();
        allocator.release(1).unwrap();
        allocator.release(2).unwrap();

        assert_eq!(allocator.allocate().unwrap(), 1);
        assert_eq!(allocator.allocate().unwrap(), 2);
        assert_eq!(allocator.allocate().unwrap(), 3);
        assert_eq!(allocator.stats().free_pages, 0);
    }

    #[test]
    fn rejects_double_free_and_unknown_page_ids() {
        let mut allocator = PageAllocator::with_existing_pages(2);
        assert!(allocator.release(2).is_err());
        allocator.release(1).unwrap();
        assert!(allocator.release(1).is_err());
        assert!(allocator.is_allocated(0));
        assert!(!allocator.is_allocated(1));
    }

    #[test]
    fn reports_free_page_count_and_high_water_mark() {
        let mut allocator = PageAllocator::with_existing_pages(8);
        allocator.release(4).unwrap();
        allocator.release(6).unwrap();

        assert_eq!(allocator.stats(), AllocatorStats {
            high_water_mark: 8,
            free_pages: 2,
        });
    }
}
