use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::SegQueue;
use stable_deref_trait::StableDeref;

use crate::poolable::{Poolable, Realloc};

/// A pool of byte slices, that reuses memory.
#[derive(Debug)]
pub struct BytePool<T = Vec<u8>>
where
    T: Poolable,
{
    list_large: SegQueue<T>,
    list_small: SegQueue<T>,
    cached_bytes: AtomicUsize,
    max_cached_bytes: Option<usize>,
}

/// The size at which point values are allocated in the small list, rather
// than the big.
const SPLIT_SIZE: usize = 4 * 1024;
const REUSE_SLACK: usize = 1024;
const PROBE_DEPTH: usize = 8;

/// The value returned by an allocation of the pool.
/// When it is dropped the memory gets returned into the pool, and is not zeroed.
/// If that is a concern, you must clear the data yourself.
pub struct Block<'a, T: Poolable = Vec<u8>> {
    data: mem::ManuallyDrop<T>,
    pool: &'a BytePool<T>,
}

impl<T: Poolable + fmt::Debug> fmt::Debug for Block<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Block").field("data", &self.data).finish()
    }
}

impl<T: Poolable> Default for BytePool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Poolable> BytePool<T> {
    /// Constructs a new pool without any cache size limit.
    pub fn new() -> Self {
        Self::with_max_cache_size_internal(None)
    }

    /// Constructs a new pool that caches at most `max_cached_bytes` bytes.
    pub fn with_max_cache_size(max_cached_bytes: usize) -> Self {
        Self::with_max_cache_size_internal(Some(max_cached_bytes))
    }

    fn with_max_cache_size_internal(max_cached_bytes: Option<usize>) -> Self {
        BytePool::<T> {
            list_large: SegQueue::new(),
            list_small: SegQueue::new(),
            cached_bytes: AtomicUsize::new(0),
            max_cached_bytes,
        }
    }

    /// Allocates a new `Block`, which represents a fixed sice byte slice.
    /// If `Block` is dropped, the memory is _not_ freed, but rather it is returned into the pool.
    /// The returned `Block` contains arbitrary data, and must be zeroed or overwritten,
    /// in cases this is needed.
    pub fn alloc(&self, size: usize) -> Block<'_, T> {
        self.alloc_internal(size, false)
    }

    pub fn alloc_and_fill(&self, size: usize) -> Block<'_, T> {
        self.alloc_internal(size, true)
    }

    pub fn alloc_internal(&self, size: usize, fill: bool) -> Block<'_, T> {
        if size == 0 {
            let data = if fill { T::alloc_and_fill(0) } else { T::alloc(0) };
            return Block::new(data, self);
        }

        let list = if size < SPLIT_SIZE {
            &self.list_small
        } else {
            &self.list_large
        };

        let mut candidates = Vec::with_capacity(PROBE_DEPTH);
        let mut best_idx = None;
        let mut best_capacity = usize::MAX;

        for idx in 0..PROBE_DEPTH {
            let Some(candidate) = self.pop_cached_block(list) else {
                break;
            };

            let capacity = candidate.capacity();
            if capacity >= size
                && capacity <= size.saturating_add(REUSE_SLACK)
                && capacity < best_capacity
            {
                best_capacity = capacity;
                best_idx = Some(idx);
            }
            candidates.push(candidate);
        }

        if let Some(best_idx) = best_idx {
            let mut reused = candidates.swap_remove(best_idx);
            for candidate in candidates {
                self.push_raw_block(candidate);
            }
            if fill {
                reused.resize(size);
            }
            return Block::new(reused, self);
        }

        for candidate in candidates {
            self.push_raw_block(candidate);
        }

        // allocate a new block
        let data = if fill {
            T::alloc_and_fill(size)
        } else {
            T::alloc(size)
        };
        Block::new(data, self)
    }

    fn push_raw_block(&self, block: T) {
        let capacity = block.capacity();
        if capacity == 0 {
            return;
        }
        if !self.try_reserve_cached_bytes(capacity) {
            return;
        }
        if capacity < SPLIT_SIZE {
            self.list_small.push(block);
        } else {
            self.list_large.push(block);
        }
    }

    fn pop_cached_block(&self, list: &SegQueue<T>) -> Option<T> {
        let block = list.pop()?;
        self.cached_bytes
            .fetch_sub(block.capacity(), Ordering::AcqRel);
        Some(block)
    }

    fn try_reserve_cached_bytes(&self, bytes: usize) -> bool {
        let Some(limit) = self.max_cached_bytes else {
            self.cached_bytes.fetch_add(bytes, Ordering::AcqRel);
            return true;
        };

        let mut current = self.cached_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > limit {
                return false;
            }
            match self.cached_bytes.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }
}

impl<T: Default + Clone> BytePool<Vec<T>> {
    /// Allocates a new block and fills it with a copy of `data`.
    pub fn alloc_from_slice(&self, data: &[T]) -> Block<'_, Vec<T>> {
        let mut block = self.alloc(data.len());
        block.extend_from_slice(data);
        block
    }
}

impl<'a, T: Poolable> Drop for Block<'a, T> {
    fn drop(&mut self) {
        let mut data = mem::ManuallyDrop::into_inner(unsafe { ptr::read(&self.data) });
        data.reset();
        self.pool.push_raw_block(data);
    }
}

impl<'a, T: Poolable> Block<'a, T> {
    fn new(data: T, pool: &'a BytePool<T>) -> Self {
        Block {
            data: mem::ManuallyDrop::new(data),
            pool,
        }
    }

    /// Returns the amount of bytes this block has.
    pub fn size(&self) -> usize {
        self.data.capacity()
    }
}

impl<'a, T: Poolable + Realloc> Block<'a, T> {
    /// Resizes a block to a new size.
    pub fn realloc(&mut self, new_size: usize) {
        self.data.realloc(new_size);
    }
}

impl<'a, T: Default + Clone> Block<'a, Vec<T>> {
    /// Updates the logical length after writing into a pre-sized buffer.
    ///
    /// This is intended for buffers created by `alloc_and_fill`, where `len()`
    /// starts at the maximum writable size. `filled_len` must not exceed the
    /// current length.
    pub fn set_filled_len(&mut self, filled_len: usize) {
        assert!(
            filled_len <= self.len(),
            "filled_len ({}) must be <= current len ({})",
            filled_len,
            self.len()
        );
        self.truncate(filled_len);
    }
}

impl<'a, T: Poolable> Deref for Block<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data.deref()
    }
}

impl<'a, T: Poolable> DerefMut for Block<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.deref_mut()
    }
}

// Safe because Block is just a wrapper around `T`.
unsafe impl<'a, T: StableDeref + Poolable> StableDeref for Block<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append() {
        let pool = BytePool::<Vec<u8>>::new();
        let mut buf = pool.alloc(4);
        assert_eq!(0, buf.len());
        assert_eq!(4, buf.capacity());
        buf.push(12u8);
        assert_eq!(1, buf.len());
        buf.extend_from_slice("hello".as_bytes());
        assert_eq!(6, buf.len());
        buf.clear();
        assert_eq!(0, buf.len());
        assert!(buf.capacity() > 0);
    }

    #[test]
    fn alloc_from_slice() {
        let pool = BytePool::<Vec<u8>>::new();
        let buf = pool.alloc_from_slice(b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..], b"hello");
    }

    #[test]
    fn len_and_capacity() {
        let pool = BytePool::<Vec<u8>>::new();
        for i in 1..10 {
            let buf = pool.alloc_and_fill(i);
            assert_eq!(buf.len(), i)
        }
        for i in 1..10 {
            let buf = pool.alloc(i);
            assert_eq!(buf.len(), 0)
        }
        for i in 1..10 {
            let buf = pool.alloc_and_fill(i * 10000);
            assert_eq!(buf.len(), i * 10000)
        }
        for i in 1..10 {
            let buf = pool.alloc(i * 10000);
            assert_eq!(buf.len(), 0)
        }
    }

    #[test]
    fn zero_sized_allocations_are_allowed_and_not_cached() {
        let pool = BytePool::<Vec<u8>>::new();

        let buf = pool.alloc(0);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 0);
        drop(buf);

        let filled = pool.alloc_and_fill(0);
        assert_eq!(filled.len(), 0);
        assert_eq!(filled.capacity(), 0);
        drop(filled);

        let from_slice = pool.alloc_from_slice(&[]);
        assert_eq!(from_slice.len(), 0);
        assert_eq!(from_slice.capacity(), 0);
        drop(from_slice);

        assert_eq!(pool.list_small.len(), 0);
        assert_eq!(pool.list_large.len(), 0);
        assert_eq!(pool.cached_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn set_filled_len() {
        let pool = BytePool::<Vec<u8>>::new();
        let mut frame = pool.alloc_and_fill(64);
        frame[10] = 123;
        frame.set_filled_len(16);
        assert_eq!(frame.len(), 16);
        assert_eq!(frame[10], 123);
    }

    #[test]
    #[should_panic(expected = "filled_len")]
    fn set_filled_len_panics_when_too_large() {
        let pool = BytePool::<Vec<u8>>::new();
        let mut frame = pool.alloc_and_fill(16);
        frame.set_filled_len(17);
    }

    #[test]
    fn basics_vec_u8() {
        let pool: BytePool<Vec<u8>> = BytePool::new();

        for i in 0..100 {
            let mut block_1k = pool.alloc(1 * 1024);
            let mut block_4k = pool.alloc(4 * 1024);

            for el in block_1k.deref_mut() {
                *el = i as u8;
            }

            for el in block_4k.deref_mut() {
                *el = i as u8;
            }

            for el in block_1k.deref() {
                assert_eq!(*el, i as u8);
            }

            for el in block_4k.deref() {
                assert_eq!(*el, i as u8);
            }
        }
    }

    #[test]
    fn realloc() {
        let pool: BytePool<Vec<u8>> = BytePool::new();

        let mut buf = pool.alloc(10);

        let _slice: &[u8] = &buf;

        assert_eq!(buf.capacity(), 10);
        buf.resize(10, 0);
        for i in 0..10 {
            buf[i] = 1;
        }

        buf.realloc(512);
        assert_eq!(buf.capacity(), 512);
        for el in buf.iter().take(10) {
            assert_eq!(*el, 1);
        }

        buf.realloc(5);
        assert_eq!(buf.capacity(), 5);
        for el in buf.iter() {
            assert_eq!(*el, 1);
        }
    }

    #[test]
    fn multi_thread() {
        let pool = std::sync::Arc::new(BytePool::<Vec<u8>>::new());

        let pool1 = pool.clone();
        let h1 = std::thread::spawn(move || {
            for _ in 0..100 {
                let mut buf = pool1.alloc_and_fill(64);
                buf[10] = 10;
            }
        });

        let pool2 = pool.clone();
        let h2 = std::thread::spawn(move || {
            for _ in 0..100 {
                let mut buf = pool2.alloc_and_fill(64);
                buf[10] = 10;
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        assert!(pool.list_small.len() <= 16);
        let cached_bytes = pool.cached_bytes.load(Ordering::Acquire);
        assert!(cached_bytes <= pool.list_small.len() * 64);
    }

    #[test]
    fn reuses_best_fit_from_multiple_candidates() {
        let pool = BytePool::<Vec<u8>>::new();

        let large = pool.alloc_and_fill(2048);
        let exact = pool.alloc_and_fill(1024);
        drop(large);
        drop(exact);

        let reused = pool.alloc(1024);
        assert_eq!(reused.capacity(), 1024);
    }

    #[test]
    fn enforces_max_cached_bytes() {
        let pool = BytePool::<Vec<u8>>::with_max_cache_size(1024);

        drop(pool.alloc_and_fill(800));
        drop(pool.alloc_and_fill(800));

        assert_eq!(pool.list_small.len(), 1);
        assert!(pool.cached_bytes.load(Ordering::Acquire) <= 1024);
    }

    #[test]
    fn basics_vec_usize() {
        let pool: BytePool<Vec<usize>> = BytePool::new();

        for i in 0..100 {
            let mut block_1k = pool.alloc(1 * 1024);
            let mut block_4k = pool.alloc(4 * 1024);

            for el in block_1k.deref_mut() {
                *el = i;
            }

            for el in block_4k.deref_mut() {
                *el = i;
            }

            for el in block_1k.deref() {
                assert_eq!(*el, i);
            }

            for el in block_4k.deref() {
                assert_eq!(*el, i);
            }
        }
    }

    #[test]
    fn basics_hash_map() {
        use std::collections::HashMap;
        let pool: BytePool<HashMap<String, String>> = BytePool::new();

        let mut map = pool.alloc(4);
        for i in 0..4 {
            map.insert(format!("hello_{}", i), "world".into());
        }
        for i in 0..4 {
            assert_eq!(
                map.get(&format!("hello_{}", i)).unwrap(),
                &"world".to_string()
            );
        }
        drop(map);

        for i in 0..100 {
            let mut block_1k = pool.alloc(1 * 1024);
            let mut block_4k = pool.alloc(4 * 1024);

            for el in block_1k.deref_mut() {
                *el.1 = i.to_string();
            }

            for el in block_4k.deref_mut() {
                *el.1 = i.to_string();
            }

            for el in block_1k.deref() {
                assert_eq!(*el.0, i.to_string());
                assert_eq!(*el.1, i.to_string());
            }

            for el in block_4k.deref() {
                assert_eq!(*el.0, i.to_string());
                assert_eq!(*el.1, i.to_string());
            }
        }
    }
}
