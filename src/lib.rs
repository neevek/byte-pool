//! Pool of byte slices.
//!
//! # Example
//! ```rust
//! use byte_pool::BytePool;
//!
//! // Create a pool
//! let pool = BytePool::<Vec<u8>>::new();
//!
//! // Allocate and copy from existing bytes.
//! let payload = pool.alloc_from_slice(b"hello");
//! assert_eq!(&payload[..], b"hello");
//!
//! // Allocate a writable frame and then shrink to received size.
//! let mut frame = pool.alloc_and_fill(1024);
//! let received = 128;
//! frame.set_filled_len(received);
//! assert_eq!(frame.len(), received);
//!
//! // Returns the underlying memory to the pool.
//! drop(payload);
//! drop(frame);
//!
//! // Frees all memory in the pool.
//! drop(pool);
//! ```

mod pool;
mod poolable;

pub use pool::{Block, BytePool};
pub use poolable::{Poolable, Realloc};
