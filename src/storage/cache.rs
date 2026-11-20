//! Bounded LRU cache for validated fixed-page payloads.

use crate::PageCacheStats;
use std::collections::{BTreeMap, HashMap};

pub(crate) const DEFAULT_PAGE_CACHE_CAPACITY: usize = 128;
pub(crate) const MAX_PAGE_CACHE_CAPACITY: usize = 65_536;

#[derive(Debug, Clone, Copy)]
struct RecencyLinks {
    previous: Option<u64>,
    next: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct PageCache {
    capacity_pages: usize,
    pages: BTreeMap<u64, Vec<u8>>,
    // Intrusive LRU links indexed by page ID. BTreeMap remains the ordered
    // directory for range scans; the hash index makes hits and touches O(1).
    recency: HashMap<u64, RecencyLinks>,
    oldest_page_id: Option<u64>,
    newest_page_id: Option<u64>,
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
            recency: HashMap::new(),
            oldest_page_id: None,
            newest_page_id: None,
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
        let existed = self.pages.insert(page_id, payload).is_some();
        if existed {
            self.touch(page_id);
        } else {
            self.append_newest(page_id);
        }
        while self.pages.len() > self.capacity_pages {
            self.evict_oldest();
        }
        debug_assert_eq!(self.pages.len(), self.recency.len());
    }

    pub(crate) fn remove_range(&mut self, start_page_id: u64, end_page_id: u64) {
        if start_page_id >= end_page_id {
            return;
        }
        let removed = self.pages.range(start_page_id..end_page_id)
            .map(|(page_id, _)| *page_id)
            .collect::<Vec<_>>();
        for page_id in removed {
            self.pages.remove(&page_id);
            self.detach(page_id);
        }
        debug_assert_eq!(self.pages.len(), self.recency.len());
    }

    pub(crate) fn set_capacity(&mut self, capacity_pages: usize) -> Result<(), crate::DbError> {
        if capacity_pages > MAX_PAGE_CACHE_CAPACITY {
            return Err(crate::DbError::Constraint(format!(
                "page-cache capacity cannot exceed {MAX_PAGE_CACHE_CAPACITY} pages"
            )));
        }
        self.capacity_pages = capacity_pages;
        while self.pages.len() > self.capacity_pages {
            self.evict_oldest();
        }
        debug_assert_eq!(self.pages.len(), self.recency.len());
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
        if self.newest_page_id == Some(page_id) {
            return;
        }
        if self.detach(page_id) {
            self.append_newest(page_id);
        }
    }

    fn append_newest(&mut self, page_id: u64) {
        debug_assert!(!self.recency.contains_key(&page_id));
        let previous = self.newest_page_id;
        self.recency.insert(page_id, RecencyLinks { previous, next: None });

        if let Some(previous_id) = previous {
            if let Some(previous_links) = self.recency.get_mut(&previous_id) {
                previous_links.next = Some(page_id);
            } else {
                debug_assert!(false, "newest LRU node must be present");
                self.oldest_page_id = Some(page_id);
            }
        } else {
            self.oldest_page_id = Some(page_id);
        }
        self.newest_page_id = Some(page_id);
    }

    fn detach(&mut self, page_id: u64) -> bool {
        let Some(links) = self.recency.remove(&page_id) else {
            return false;
        };

        if let Some(previous_id) = links.previous {
            if let Some(previous_links) = self.recency.get_mut(&previous_id) {
                previous_links.next = links.next;
            }
        } else {
            self.oldest_page_id = links.next;
        }

        if let Some(next_id) = links.next {
            if let Some(next_links) = self.recency.get_mut(&next_id) {
                next_links.previous = links.previous;
            }
        } else {
            self.newest_page_id = links.previous;
        }
        true
    }

    fn evict_oldest(&mut self) {
        let Some(oldest) = self.oldest_page_id else {
            debug_assert!(self.pages.is_empty(), "non-empty page map must have an LRU head");
            return;
        };
        self.pages.remove(&oldest);
        self.detach(oldest);
        self.evictions = self.evictions.saturating_add(1);
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
    fn updating_a_cached_page_refreshes_its_lru_position() {
        let mut cache = PageCache::new(2).unwrap();
        cache.insert(1, b"one".to_vec());
        cache.insert(2, b"two".to_vec());
        cache.insert(1, b"one-updated".to_vec());
        cache.insert(3, b"three".to_vec());

        assert_eq!(cache.get(1), Some(b"one-updated".to_vec()));
        assert_eq!(cache.get(2), None);
        assert_eq!(cache.get(3), Some(b"three".to_vec()));
    }

    #[test]
    fn cache_rejects_excessive_capacity() {
        assert!(PageCache::new(65_537).is_err());
    }
}
