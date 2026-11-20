//! Bounded LRU cache for validated fixed-page payloads.

use crate::PageCacheStats;
use std::collections::{BTreeMap, VecDeque};

pub(crate) const DEFAULT_PAGE_CACHE_CAPACITY: usize = 128;
pub(crate) const MAX_PAGE_CACHE_CAPACITY: usize = 65_536;

#[derive(Debug)]
pub(crate) struct PageCache {
    capacity_pages: usize,
    pages: BTreeMap<u64, Vec<u8>>,
    recency: VecDeque<u64>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl PageCache {
    pub(crate) fn new(capacity_pages: usize) -> Result<Self, crate::DbError> {
        if capacity_pages > MAX_PAGE_CACHE_CAPACITY {
            return Err(crate::DbError::Constraint(format!(
                "page-cache capacity cannot exceed {MAX_PAGE_CACHE_CAPACITY} pages"
            )));
        }
        Ok(Self {
            capacity_pages,
            pages: BTreeMap::new(),
            recency: VecDeque::new(),
            hits: 0,
            misses: 0,
            evictions: 0,
        })
    }

    pub(crate) fn get(&mut self, page_id: u64) -> Option<Vec<u8>> {
        let Some(payload) = self.pages.get(&page_id).cloned() else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        self.hits = self.hits.saturating_add(1);
        self.touch(page_id);
        Some(payload)
    }

    pub(crate) fn insert(&mut self, page_id: u64, payload: Vec<u8>) {
        if self.capacity_pages == 0 {
            return;
        }
        self.pages.insert(page_id, payload);
        self.touch(page_id);
        while self.pages.len() > self.capacity_pages {
            let Some(oldest) = self.recency.pop_front() else { break; };
            if self.pages.remove(&oldest).is_some() {
                self.evictions = self.evictions.saturating_add(1);
            }
        }
    }

    pub(crate) fn remove_range(&mut self, start_page_id: u64, end_page_id: u64) {
        if start_page_id >= end_page_id {
            return;
        }
        let removed = self.pages.range(start_page_id..end_page_id)
            .map(|(page_id, _)| *page_id)
            .collect::<Vec<_>>();
        for page_id in &removed {
            self.pages.remove(page_id);
        }
        if !removed.is_empty() {
            // Every cached ID in this half-open range was removed above.
            // Comparing bounds is O(1) per LRU node; Vec::contains here made
            // range invalidation O(cache_capacity * removed_pages).
            self.recency.retain(|page_id| {
                *page_id < start_page_id || *page_id >= end_page_id
            });
        }
    }

    pub(crate) fn set_capacity(&mut self, capacity_pages: usize) -> Result<(), crate::DbError> {
        if capacity_pages > MAX_PAGE_CACHE_CAPACITY {
            return Err(crate::DbError::Constraint(format!(
                "page-cache capacity cannot exceed {MAX_PAGE_CACHE_CAPACITY} pages"
            )));
        }
        self.capacity_pages = capacity_pages;
        while self.pages.len() > self.capacity_pages {
            let Some(oldest) = self.recency.pop_front() else { break; };
            if self.pages.remove(&oldest).is_some() {
                self.evictions = self.evictions.saturating_add(1);
            }
        }
        Ok(())
    }

    pub(crate) fn stats(&self) -> PageCacheStats {
        PageCacheStats {
            capacity_pages: self.capacity_pages,
            resident_pages: self.pages.len(),
            resident_bytes: self.pages.values().map(Vec::len).sum(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }

    fn touch(&mut self, page_id: u64) {
        self.recency.retain(|existing| *existing != page_id);
        self.recency.push_back(page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::PageCache;

    #[test]
    fn cache_uses_lru_eviction_and_reports_metrics() {
        let mut cache = PageCache::new(2).unwrap();
        cache.insert(1, b"one".to_vec());
        cache.insert(2, b"two".to_vec());

        assert_eq!(cache.get(1), Some(b"one".to_vec()));
        cache.insert(3, b"three".to_vec());

        assert_eq!(cache.get(1), Some(b"one".to_vec()));
        assert_eq!(cache.get(2), None);
        assert_eq!(cache.get(3), Some(b"three".to_vec()));

        let stats = cache.stats();
        assert_eq!(stats.capacity_pages, 2);
        assert_eq!(stats.resident_pages, 2);
        assert_eq!(stats.resident_bytes, 8);
        assert_eq!(stats.hits, 3);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn cache_can_be_disabled_and_capacity_reduction_evicts_oldest() {
        let mut cache = PageCache::new(2).unwrap();
        cache.insert(1, b"one".to_vec());
        cache.insert(2, b"two".to_vec());
        assert_eq!(cache.get(1), Some(b"one".to_vec()));
        cache.set_capacity(1).unwrap();
        assert_eq!(cache.get(1), Some(b"one".to_vec()));
        assert_eq!(cache.get(2), None);

        cache.set_capacity(0).unwrap();
        cache.insert(3, b"three".to_vec());
        assert_eq!(cache.get(3), None);
        assert_eq!(cache.stats().resident_pages, 0);
    }

    #[test]
    fn range_removal_keeps_lru_order_consistent_for_reinserted_pages() {
        let mut cache = PageCache::new(3).unwrap();
        cache.insert(10, b"ten".to_vec());
        cache.insert(11, b"eleven".to_vec());
        cache.insert(12, b"twelve".to_vec());

        assert_eq!(cache.get(10), Some(b"ten".to_vec()));
        cache.remove_range(11, 13);

        assert_eq!(cache.get(11), None);
        assert_eq!(cache.get(12), None);
        assert_eq!(cache.get(10), Some(b"ten".to_vec()));

        // Reusing a page ID after invalidation must create exactly one fresh
        // recency entry rather than being shadowed by a stale queue entry.
        cache.insert(12, b"twelve-new".to_vec());
        cache.insert(13, b"thirteen".to_vec());
        cache.insert(14, b"fourteen".to_vec());

        assert_eq!(cache.get(10), None);
        assert_eq!(cache.get(12), Some(b"twelve-new".to_vec()));
        assert_eq!(cache.get(13), Some(b"thirteen".to_vec()));
        assert_eq!(cache.get(14), Some(b"fourteen".to_vec()));
        assert_eq!(cache.stats().resident_pages, 3);
    }

    #[test]
    fn cache_rejects_excessive_capacity() {
        assert!(PageCache::new(65_537).is_err());
    }
}
