<h1 align="center">byte-pool</h1>
<div align="center">
 <strong>
   A flexible byte pool.
 </strong>
</div>

<br />

<div align="center">
  <!-- Crates version -->
  <a href="https://crates.io/crates/byte-pool">
    <img src="https://img.shields.io/crates/v/byte-pool.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/byte-pool">
    <img src="https://img.shields.io/crates/d/byte-pool.svg?style=flat-square"
      alt="Download" />
  </a>
  <!-- docs.rs docs -->
  <a href="https://docs.rs/byte-pool">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
  <!-- CI -->
  <a href="https://github.com/dignifiedquire/byte-pool/actions">
    <img src="https://github.com/dignifiedquire/byte-pool/workflows/CI/badge.svg"
      alt="CI status" />
  </a>
</div>

<div align="center">
  <h3>
    <a href="https://docs.rs/byte-pool">
      API Docs
    </a>
    <span> | </span>
    <a href="https://github.com/dignifiedquire/byte-pool/releases">
      Releases
    </a>
  </h3>
</div>

<br/>

## Example

```rust
use byte_pool::BytePool;

// Create a pool
let pool = BytePool::<Vec<u8>>::new();

// Allocate and copy from existing bytes.
let payload = pool.alloc_from_slice(b"hello");
assert_eq!(&payload[..], b"hello");

// Allocate a writable frame and shrink it after recv/write.
let mut frame = pool.alloc_and_fill(1024);
let received = 128;
frame.set_filled_len(received);
assert_eq!(frame.len(), received);

// Returns the underlying memory to the pool.
drop(payload);
drop(frame);

// Frees all memory in the pool.
drop(pool);
```


## License

Licensed under either of
 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
