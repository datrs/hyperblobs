# Hyperblobs

A peer-to-peer blob store built on Hypercore. Rust implementation of the [JavaScript version](https://github.com/holepunchto/hyperblobs).

```
$ cargo add hyperblobs hypercore@0.17.0-alpha.1
```

# Usage

```rust
use hyperblobs::{GetOptions, HyperblobsBuilder, PutOptions};
use hypercore::{HypercoreBuilder, Storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let core = HypercoreBuilder::new(Storage::new_memory().await?)
        .build()
        .await?;
    let blobs = HyperblobsBuilder::default().core(core).build()?;

    // Store a blob, which returns the id used to read it back
    let id = blobs.put(b"hello world", &PutOptions::default()).await?;

    // Get the whole blob
    let blob = blobs.get(&id, &GetOptions::default()).await?;
    assert_eq!(blob, b"hello world");

    // Or just a byte range of it
    let range = GetOptions {
        start: Some(6),
        length: Some(5),
        ..Default::default()
    };
    assert_eq!(blobs.get(&id, &range).await?, b"world");

    Ok(())
}
```

Use `Hyperblobs::read_stream` to stream a large blob instead of loading it into memory.

# Releasing

```
$ cargo release <version> --execute
```
