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
    buckets: [SegQueue<T>; NUM_SIZE_CLASSES],
    cached_bytes: AtomicUsize,
    max_cached_bytes: Option<usize>,
}

/// Number of power-of-two size class buckets.
const NUM_SIZE_CLASSES: usize = 16;
/// Sizes below 2^MIN_SIZE_CLASS_BITS all land in bucket 0.
const MIN_SIZE_CLASS_BITS: usize = 8;
const PROBE_DEPTH: usize = 8;

/// Maps a non-zero capacity to a bucket index.
/// Bucket 0 covers [1, 255], bucket 1 covers [256, 511], and so on,
/// each doubling in range. Bucket 15 captures everything above 4 MiB.
#[inline]
fn size_class(size: usize) -> usize {
    debug_assert!(size > 0);
    let bits = (usize::BITS as usize) - (size.leading_zeros() as usize);
    bits.saturating_sub(MIN_SIZE_CLASS_BITS).min(NUM_SIZE_CLASSES - 1)
}

/// Returns the maximum size that maps to the same size class as `size`,
/// used by `alloc_from_slice` to pre-size blocks so they serve the widest
/// range of future requests within the same bucket.
///
/// For sizes in the capped top class the input is returned unchanged —
/// rounding up would double or more the allocation (e.g. 5 MiB → 8 MiB).
#[inline]
fn class_slab_size(size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    let bits = (usize::BITS as usize) - (size.leading_zeros() as usize);
    let class = bits.saturating_sub(MIN_SIZE_CLASS_BITS).min(NUM_SIZE_CLASSES - 1);
    if class == NUM_SIZE_CLASSES - 1 {
        return size;
    }
    if bits <= MIN_SIZE_CLASS_BITS {
        (1_usize << MIN_SIZE_CLASS_BITS) - 1
    } else {
        (1_usize << bits) - 1
    }
}

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
            buckets: std::array::from_fn(|_| SegQueue::new()),
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

        let bucket_idx = size_class(size);

        // Primary probe. If that misses, probe the next bucket as a fallback:
        // some Poolable allocators (e.g. HashMap) report a capacity larger than
        // requested, so a block dropped after alloc(N) can land one class higher
        // than the class future alloc(N) calls probe.
        if let Some(data) = self.try_alloc_from_bucket(bucket_idx, size, fill) {
            return Block::new(data, self);
        }
        if bucket_idx + 1 < NUM_SIZE_CLASSES {
            if let Some(data) = self.try_alloc_from_bucket(bucket_idx + 1, size, fill) {
                return Block::new(data, self);
            }
        }

        let data = if fill {
            T::alloc_and_fill(size)
        } else {
            T::alloc(size)
        };
        Block::new(data, self)
    }

    /// Probes a single bucket for the best-fit block that satisfies `size`.
    /// Returns `Some(data)` on a hit (all other candidates are pushed back),
    /// or `None` on a miss (all candidates are pushed back).
    fn try_alloc_from_bucket(&self, bucket_idx: usize, size: usize, fill: bool) -> Option<T> {
        // Bucket 15 is the unbounded catch-all; cap reuse at 4× to prevent
        // returning, e.g., a 1 GiB cached block for a 4 MiB request.
        let reuse_upper = if bucket_idx == NUM_SIZE_CLASSES - 1 {
            size.saturating_mul(4)
        } else {
            usize::MAX
        };

        let bucket = &self.buckets[bucket_idx];
        let mut candidates = Vec::with_capacity(PROBE_DEPTH);
        let mut best_idx = None;
        let mut best_capacity = usize::MAX;

        for idx in 0..PROBE_DEPTH {
            let Some(candidate) = self.pop_cached_block(bucket) else {
                break;
            };
            let capacity = candidate.capacity();
            // Best-fit: smallest block that is large enough and within the reuse bound.
            if capacity >= size && capacity <= reuse_upper && capacity < best_capacity {
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
            Some(reused)
        } else {
            for candidate in candidates {
                self.push_raw_block(candidate);
            }
            None
        }
    }

    fn push_raw_block(&self, block: T) {
        let capacity = block.capacity();
        if capacity == 0 {
            return;
        }
        if !self.try_reserve_cached_bytes(capacity) {
            return;
        }
        self.buckets[size_class(capacity)].push(block);
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

    #[cfg(test)]
    fn total_cached_count(&self) -> usize {
        self.buckets.iter().map(|q| q.len()).sum()
    }
}

impl<T: Default + Clone> BytePool<Vec<T>> {
    /// Allocates a new block and fills it with a copy of `data`.
    ///
    /// The block is sized to the upper bound of the byte-size class for `data`,
    /// so that future calls with similarly-sized slices are more likely to hit
    /// a cached block.
    pub fn alloc_from_slice(&self, data: &[T]) -> Block<'_, Vec<T>> {
        let alloc_size = slab_elem_count::<T>(data.len());
        let mut block = self.alloc(alloc_size);
        block.extend_from_slice(data);
        block
    }
}

/// Converts an element count to a slab-rounded element count using byte-based
/// size classes, so that `alloc_from_slice` over-allocates proportionally to
/// the element byte size rather than treating element count as a byte count.
fn slab_elem_count<T>(len: usize) -> usize {
    let elem_size = std::mem::size_of::<T>();
    if elem_size == 0 || len == 0 {
        return len;
    }
    let byte_len = len.saturating_mul(elem_size);
    let slab_bytes = class_slab_size(byte_len);
    (slab_bytes + elem_size - 1) / elem_size
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

        assert_eq!(pool.total_cached_count(), 0);
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

        let count = pool.total_cached_count();
        assert!(count <= 32);
        let cached_bytes = pool.cached_bytes.load(Ordering::Acquire);
        assert!(cached_bytes <= count * 64);
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

        assert_eq!(pool.total_cached_count(), 1);
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

    #[test]
    fn size_class_buckets() {
        assert_eq!(size_class(1), 0);
        assert_eq!(size_class(255), 0);
        assert_eq!(size_class(256), 1);
        assert_eq!(size_class(511), 1);
        assert_eq!(size_class(512), 2);
        assert_eq!(size_class(1024), 3);
        assert_eq!(size_class(4096), 5);
        assert_eq!(size_class(usize::MAX), NUM_SIZE_CLASSES - 1);
    }

    #[test]
    fn class_slab_size_rounds_up() {
        assert_eq!(class_slab_size(0), 0);
        assert_eq!(class_slab_size(1), 255);
        assert_eq!(class_slab_size(255), 255);
        assert_eq!(class_slab_size(256), 511);
        assert_eq!(class_slab_size(300), 511);
        assert_eq!(class_slab_size(511), 511);
        assert_eq!(class_slab_size(512), 1023);
        // Top class (>= 4 MiB): returned unchanged to avoid doubling large allocations.
        assert_eq!(class_slab_size(4 * 1024 * 1024), 4 * 1024 * 1024);
        assert_eq!(class_slab_size(5 * 1024 * 1024), 5 * 1024 * 1024);
    }

    #[test]
    fn alloc_from_slice_reuses_across_similar_sizes() {
        let pool = BytePool::<Vec<u8>>::new();

        // alloc_from_slice(5) sizes up to class_slab_size(5) = 255
        let buf = pool.alloc_from_slice(b"hello");
        assert_eq!(buf.len(), 5);
        drop(buf); // returns a 255-capacity block to bucket 0

        // A later alloc_from_slice with different size in the same class reuses it
        let buf2 = pool.alloc_from_slice(b"world!!");
        assert_eq!(buf2.len(), 7);
        // capacity should be from the reused block (255), not freshly allocated
        assert!(buf2.capacity() >= 255);
    }

    #[test]
    fn blocks_route_to_correct_buckets() {
        let pool = BytePool::<Vec<u8>>::new();

        let b1 = pool.alloc(256); // class 1
        let b2 = pool.alloc(512); // class 2
        let b3 = pool.alloc(1024); // class 3
        drop(b1);
        drop(b2);
        drop(b3);

        // Each size class has exactly one block
        assert_eq!(pool.buckets[1].len(), 1); // 256 → class 1
        assert_eq!(pool.buckets[2].len(), 1); // 512 → class 2
        assert_eq!(pool.buckets[3].len(), 1); // 1024 → class 3
        assert_eq!(pool.total_cached_count(), 3);
    }

    #[test]
    fn alloc_from_slice_large_element_does_not_over_allocate() {
        // T = u64 (8 bytes). Without byte-based sizing, class_slab_size(data.len())
        // = class_slab_size(1) = 255 elements = 2040 bytes.
        // With the fix the slab is computed in bytes:
        //   byte_len = 8, class_slab_size(8) = 255, ceil(255/8) = 32 elements = 256 bytes.
        let pool = BytePool::<Vec<u64>>::new();
        let buf = pool.alloc_from_slice(&[42u64]);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 42u64);
        // Capacity must be byte-proportional (≤ 32 elements), not element-count-
        // proportional (the old broken ceiling of 255 elements).
        assert!(
            buf.capacity() < 255,
            "capacity should be byte-proportional: got {}",
            buf.capacity()
        );
    }

    #[test]
    fn reuses_allocator_rounded_block_via_adjacent_bucket() {
        // Allocators such as HashMap may round up the reported capacity beyond
        // the requested size, landing the returned block in a higher bucket than
        // future alloc(N) calls probe. Verify that the fallback probe recovers it.
        use std::collections::HashMap;
        let pool: BytePool<HashMap<u64, u64>> = BytePool::new();

        let block = pool.alloc(255);
        let actual_cap = block.capacity(); // may be > 255 due to HashMap rounding
        drop(block);

        // The next alloc(255) must reuse the cached block, not allocate fresh.
        let block2 = pool.alloc(255);
        assert_eq!(
            block2.capacity(),
            actual_cap,
            "block should be reused even when capacity rounded into the next class"
        );
        assert_eq!(pool.total_cached_count(), 0, "cache should be empty after reuse");
    }
}
