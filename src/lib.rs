/*!
Store binary blobs with hypercore. Inspired by the [JavaScript implementation](https://github.com/holepunchto/hyperblobs).
```rust
# println!("This line not shown in the blocks");
println!("This line is");
```
*/
#![warn(
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    unsafe_code,
    non_local_definitions
)]

use std::time::Duration;

use derive_builder::Builder;
use futures_lite::{Stream, StreamExt};
use hypercore::{Hypercore, HypercoreError};
use hypercore_handshake::CipherTrait;
use serde::{Deserialize, Serialize};
use tracing::instrument;

const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
/// Errors for [`Hyperblobs`].
pub enum Error {
    #[error("Error from hypercore")]
    /// Error from hypercore
    HypercoreError(#[from] HypercoreError),
    /// A block making up the blob is not present locally and could not be downloaded.
    /// The equivalent of JS hyperblobs' `BLOCK_NOT_AVAILABLE`.
    #[error("Block [{0}] of the blob is not available")]
    BlockNotAvailable(u64),
    /// A block was shorter than the range being read requires it to be, so the read could make no
    /// progress. [`Hyperblobs::read_stream`] locates the block holding a range's first byte by
    /// assuming the blob is chunked at a fixed size, which is true of anything [`Hyperblobs::put`]
    /// wrote. This is what a core whose blocks were appended by hand at varying sizes looks like
    /// from the inside.
    #[error("Block [{0}] of the blob is shorter than expected")]
    ShortBlock(u64),
}
type Result<T> = std::result::Result<T, Error>;

/// Options for [`Hyperblobs::put`].
///
/// JS's README also documents `start`, `end` and `length` here, but its `put` never reads them: it
/// consults `opts.blockSize` and then chunks the whole buffer from zero. They are left out rather
/// than invented, and in Rust slicing the input at the call site says the same thing.
#[derive(Debug, Clone, Default, Builder)]
pub struct PutOptions {
    /// Chunk this blob at a different size than the store's own [`Hyperblobs::block_size`], the
    /// equivalent of JS's `opts.blockSize`. A store can hold blobs written at a mix of sizes: a
    /// large block size suits a media enclosure being streamed, a small one suits metadata.
    ///
    /// `Some(0)` is treated as unset, matching JS, where `0` is falsy and falls through to the
    /// store's block size.
    pub block_size: Option<u64>,
    // We omit the `core` option that JS has.
}

/// Options for [`Hyperblobs::get`] and [`Hyperblobs::read_stream`].
///
/// `start`, `length` and `end` select a byte range *within the blob*, and are the equivalent of
/// JS's options of the same names. Leaving all three unset reads the whole blob.
///
/// The range is clamped to the blob, and a range that is empty after clamping reads nothing. JS
/// does not clamp: because one hypercore holds many blobs back to back, an over-long
/// `length`/`end` there reads on into the *next* blob's blocks (and, past the end of the core,
/// never resolves at all). Clamping keeps a read inside the blob it asked for. It is also the
/// only option here, since [`GetBlob`] carries no `byte_offset`/`block_length` with which to
/// describe anything beyond the blob.
#[derive(Debug, Clone, Default, Builder)]
pub struct GetOptions {
    /// How long to wait for a block to download. If `None`, do not download.
    /// NB: this default comes from JS, where `wait = true` but `timeout = 0`.
    /// How is that different from just `wait = false`?
    /// That combo seems like it has the effect of not returning the block when not local
    /// but still sending out a request for the block, so it would get downloaded for later?
    /// TODO experiment and explore js to figure this out
    /// TODO not honored yet: `hypercore::Hypercore::get` has no wait/timeout parameter.
    pub timeout: Option<Duration>,
    /// Relative byte offset within the blob to start reading at. Defaults to `0`.
    pub start: Option<u64>,
    /// Number of bytes to read, counting from `start`. Takes precedence over `end`, matching JS,
    /// where the two are an if/else chain rather than mutually exclusive.
    pub length: Option<u64>,
    /// Relative byte offset within the blob of the last byte to read, **inclusive** (as in JS).
    /// Ignored when `length` is set.
    pub end: Option<u64>,
}

impl GetOptions {
    /// Resolve these options against a blob of `byte_length` bytes into the half-open,
    /// blob-relative byte range `start..end` to read, clamped to the blob as described on
    /// [`GetOptions`].
    fn range(&self, byte_length: u64) -> (u64, u64) {
        let start = self.start.unwrap_or(0).min(byte_length);
        let end = match (self.length, self.end) {
            (Some(length), _) => start.saturating_add(length),
            (None, Some(end)) => end.saturating_add(1),
            (None, None) => byte_length,
        };
        (start, end.clamp(start, byte_length))
    }
}

/// Result of [`Hyperblobs::put`] used for fetching data with [`Hyperblobs::get`].
///
/// Serialized as `camelCase` JSON to match the shape JS's `hyperblobs` uses when this id is
/// stored as a value elsewhere (e.g. hyper-rss's `KeyedBlobs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutBlob {
    /// Number of bytes before the blob
    pub byte_offset: u64,
    /// starting block of the putted data
    pub block_offset: u64,
    /// length in blocks of the putted data
    pub block_length: u64,
    /// length of put data in bytes
    pub byte_length: u64,
}

/// Arguments for getting a blob
#[derive(Debug, Clone)]
pub struct GetBlob {
    /// starting block of the putted data
    pub block_offset: u64,
    /// Number of bytes to get
    pub byte_length: u64,
}

impl From<&PutBlob> for GetBlob {
    fn from(
        &PutBlob {
            block_offset,
            byte_length,
            ..
        }: &PutBlob,
    ) -> Self {
        Self {
            block_offset,
            byte_length,
        }
    }
}

// this is dumb
impl From<&GetBlob> for GetBlob {
    fn from(value: &GetBlob) -> Self {
        value.clone()
    }
}

#[allow(missing_docs)]
#[derive(Debug, Builder)]
/// Blob store for hypercore
pub struct Hyperblobs {
    /// The block size that will be used when storing blobs
    #[builder(default = "DEFAULT_BLOCK_SIZE")]
    block_size: u64,
    core: Hypercore,
}

impl HyperblobsBuilder {
    //fn storage(&mut self, store: Storage)
}

impl Hyperblobs {
    /// The [`Hypercore`] the blobs are stored in, the equivalent of JS's `blobs.core`.
    ///
    /// Blobs are stored back to back as plain blocks, so this is how you get at the raw blocks a
    /// blob is made of, or at the core's key in order to replicate it.
    pub fn core(&self) -> &Hypercore {
        &self.core
    }

    /// The default size blobs are chunked into blocks at, the equivalent of JS's
    /// `blobs.blockSize`.
    ///
    /// This is a *write* setting only, and [`PutOptions::block_size`] overrides it per blob.
    /// Reading does not consult it: [`Hyperblobs::read_stream`] recovers the size a blob was
    /// written at from the blob itself, so one store reads back blobs written at any block size.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Add a replication stream for the underlying Hypercore.
    pub async fn add_stream(&self, stream: impl CipherTrait + 'static) -> Result<()> {
        tokio::spawn(self.core.replicate(stream));
        Ok(())
    }

    /// Insert a blob into the store, chunked at [`PutOptions::block_size`] or, failing that, the
    /// store's own [`Hyperblobs::block_size`].
    #[instrument]
    pub async fn put(&self, blob: &[u8], opts: &PutOptions) -> Result<PutBlob> {
        // As in JS, a `block_size` of zero means "unset" rather than a division by zero.
        let block_size = opts
            .block_size
            .filter(|b| *b > 0)
            .unwrap_or(self.block_size);
        let blocks: Vec<Vec<u8>> = blob.chunks(block_size as usize).map(Vec::from).collect();

        let byte_length = blob.len() as u64;
        let block_length = blocks.len() as u64;

        let res = self.core.append_batch(&blocks).await?;

        let out = PutBlob {
            byte_offset: res.byte_length - byte_length,
            block_offset: res.length - block_length,
            block_length,
            byte_length,
        };
        Ok(out)
    }

    /// Get a blob, or the byte range of it selected by `opts`, from the store.
    ///
    /// This is [`Hyperblobs::read_stream`] collected into one buffer, exactly as JS's `get` is
    /// defined in terms of `createReadStream`.
    #[instrument]
    pub async fn get<T: Into<GetBlob> + std::fmt::Debug>(
        &self,
        id: T,
        opts: &GetOptions,
    ) -> Result<Vec<u8>> {
        let mut out = vec![];
        let mut stream = std::pin::pin!(self.read_stream(id, opts));
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    /// Stream the requested blob, or the byte range of it selected by `opts`, in order, one chunk
    /// per underlying hypercore block, without materializing the whole blob.
    ///
    /// The equivalent of JS's `createReadStream`, and of hyper-rss's `KeyedBlobs.getRange` when
    /// `opts` carries a range. Intended for serving large blobs (audio/video enclosures)
    /// incrementally.
    ///
    /// The first and last chunks are partial blocks when the range starts or ends mid-block; the
    /// chunks concatenated are the requested bytes and nothing more. A range that is empty after
    /// clamping (see [`GetOptions`]) yields no chunks at all.
    ///
    /// The stream ends after yielding an [`Err`]; it will not retry the failed block.
    ///
    /// The store's own [`Hyperblobs::block_size`] is not consulted: the size a blob was chunked at
    /// is recovered from the blob, so a blob written with [`PutOptions::block_size`], or by a
    /// differently configured store, still reads back correctly.
    pub fn read_stream<T: Into<GetBlob>>(
        &self,
        id: T,
        opts: &GetOptions,
    ) -> impl Stream<Item = Result<Vec<u8>>> + '_ {
        let GetBlob {
            block_offset,
            byte_length,
        } = id.into();
        let (start, end) = opts.range(byte_length);

        let state = ReadState {
            core: &self.core,
            seek: (start > 0).then_some(start),
            index: block_offset,
            offset: 0,
            read: 0,
            len: end - start,
        };

        futures_lite::stream::unfold(state, |mut state| async move {
            if state.read >= state.len {
                return None;
            }

            // Locate the block holding the range's first byte. JS does this with `core.seek`,
            // which Rust's `Hypercore` has no equivalent of, so it is done arithmetically
            // instead. A blob is chunked at one fixed size, which makes every block but its last
            // exactly that size -- so the blob's own first block is what says what size it was
            // written at, whatever this store is configured with now. Skipped entirely when the
            // range starts at byte zero, which needs no seek.
            let mut located = None;
            if let Some(start) = state.seek.take() {
                let first = match state.core.get(state.index).await {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        let index = state.index;
                        return Some((Err(Error::BlockNotAvailable(index)), state.done()));
                    }
                    Err(e) => return Some((Err(Error::from(e)), state.done())),
                };
                let block_size = first.len() as u64;
                if block_size == 0 {
                    let index = state.index;
                    return Some((Err(Error::ShortBlock(index)), state.done()));
                }
                let skip = start / block_size;
                state.index += skip;
                state.offset = start % block_size;
                // The wanted byte is in the block just read, so don't fetch it a second time.
                if skip == 0 {
                    located = Some(first);
                }
            }

            let index = state.index;
            let block = match located {
                Some(block) => block,
                None => match state.core.get(index).await {
                    Ok(Some(block)) => block,
                    Ok(None) => return Some((Err(Error::BlockNotAvailable(index)), state.done())),
                    Err(e) => return Some((Err(Error::from(e)), state.done())),
                },
            };

            let offset = state.offset as usize;
            let remaining = (state.len - state.read) as usize;
            let take = block.len().saturating_sub(offset).min(remaining);
            if take == 0 {
                // Would make no progress, so the loop has to stop rather than spin.
                return Some((Err(Error::ShortBlock(index)), state.done()));
            }

            let chunk = if offset == 0 && take == block.len() {
                block
            } else {
                block[offset..(offset + take)].to_vec()
            };

            state.index += 1;
            state.offset = 0;
            state.read += take as u64;
            Some((Ok(chunk), state))
        })
    }
}

/// Cursor threaded through the stream [`Hyperblobs::read_stream`] returns.
struct ReadState<'a> {
    core: &'a Hypercore,
    /// Blob-relative offset of the range's first byte, until the block holding it has been
    /// located. `None` once located, and for a range starting at byte zero, which needs no seek.
    /// Locating costs a block read, so it happens on the stream's first poll rather than up front.
    seek: Option<u64>,
    /// Index of the next hypercore block to read.
    index: u64,
    /// Byte offset within that block at which the wanted bytes start.
    offset: u64,
    /// Bytes yielded so far.
    read: u64,
    /// Total bytes to yield.
    len: u64,
}

impl ReadState<'_> {
    /// Mark the range as fully consumed, so the stream terminates on the next poll.
    fn done(mut self) -> Self {
        self.read = self.len;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypercore::{HypercoreBuilder, Storage};

    async fn ram_blobs(block_size: u64) -> Hyperblobs {
        let core = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
            .build()
            .await
            .unwrap();
        HyperblobsBuilder::default()
            .block_size(block_size)
            .core(core)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let blobs = ram_blobs(2).await;
        let data = b"Hello, world!";
        let id = blobs.put(data, &Default::default()).await?;
        assert_eq!(id.byte_offset, 0);
        assert_eq!(id.block_offset, 0);
        assert_eq!(id.block_length, data.len().div_ceil(2) as u64);
        assert_eq!(id.byte_length, data.len() as u64);

        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let res = blobs
            .get(
                &GetBlob {
                    block_offset: 0,
                    byte_length: 5,
                },
                &Default::default(),
            )
            .await?;
        assert_eq!(res, b"Hello");

        let data2 = b"More data!!!!?";
        let id2 = blobs.put(data2, &Default::default()).await?;
        assert_eq!(id2.byte_offset, id.byte_length);
        assert_eq!(id2.block_offset, id.block_length);
        assert_eq!(id2.block_length, data2.len().div_ceil(2) as u64);
        dbg!(id2.block_length);
        assert_eq!(id2.byte_length, data2.len() as u64);
        dbg!(&id2);
        let res = blobs.get(&id2, &Default::default()).await?;
        println!("{}", String::from_utf8_lossy(&res));
        assert_eq!(res, data2);

        Ok(())
    }

    #[tokio::test]
    async fn remainders() -> Result<()> {
        let blobs = ram_blobs(3).await;

        let data = b"abcefg";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let data = b"abcef";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let data = b"abce";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);
        Ok(())
    }

    /// Collect a [`Hyperblobs::read_stream`] into its chunks, so tests can assert on the chunk
    /// boundaries and not just the concatenated bytes.
    async fn chunks(blobs: &Hyperblobs, id: &PutBlob, opts: &GetOptions) -> Result<Vec<Vec<u8>>> {
        let mut stream = std::pin::pin!(blobs.read_stream(id, opts));
        let mut out = vec![];
        while let Some(chunk) = stream.next().await {
            out.push(chunk?);
        }
        Ok(out)
    }

    fn opts(start: Option<u64>, length: Option<u64>, end: Option<u64>) -> GetOptions {
        GetOptions {
            start,
            length,
            end,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn read_stream_yields_one_chunk_per_block() -> Result<()> {
        let blobs = ram_blobs(4).await;
        let data = b"abcdefghij"; // 4 + 4 + 2
        let id = blobs.put(data, &Default::default()).await?;

        let got = chunks(&blobs, &id, &Default::default()).await?;
        assert_eq!(
            got,
            vec![b"abcd".to_vec(), b"efgh".to_vec(), b"ij".to_vec()]
        );
        Ok(())
    }

    /// The Rust port of JS's "can seek to start/length within one blob, multiple blocks":
    /// blockSize 10, a 20 byte blob, `{start: 8, length: 4}` reads across the block boundary.
    #[tokio::test]
    async fn read_stream_range_across_block_boundary() -> Result<()> {
        let blobs = ram_blobs(10).await;
        let mut data = vec![b'a'; 10];
        data.extend([b'b'; 10]);
        let id = blobs.put(&data, &Default::default()).await?;

        let o = opts(Some(8), Some(4), None);
        // Split at the block boundary: the tail of block 0, then the head of block 1.
        assert_eq!(
            chunks(&blobs, &id, &o).await?,
            vec![b"aa".to_vec(), b"bb".to_vec()]
        );
        assert_eq!(blobs.get(&id, &o).await?, b"aabb");
        Ok(())
    }

    /// Same, but with an earlier blob in the core first, so `block_offset` is non-zero. Mirrors
    /// JS's "...multiple blocks, multiple blobs" test.
    #[tokio::test]
    async fn read_stream_range_with_preceding_blob() -> Result<()> {
        let blobs = ram_blobs(10).await;
        blobs.put(&[b'z'; 50], &Default::default()).await?;

        let mut data = vec![b'a'; 10];
        data.extend([b'b'; 10]);
        let id = blobs.put(&data, &Default::default()).await?;
        assert_eq!(id.block_offset, 5);

        let got = blobs.get(&id, &opts(Some(8), Some(4), None)).await?;
        assert_eq!(got, b"aabb");
        Ok(())
    }

    #[tokio::test]
    async fn read_stream_inclusive_end_and_length_precedence() -> Result<()> {
        let blobs = ram_blobs(64).await;
        let data = b"abcdefghij";
        let id = blobs.put(data, &Default::default()).await?;

        // `end` is inclusive, as in JS.
        assert_eq!(blobs.get(&id, &opts(Some(2), None, Some(4))).await?, b"cde");
        // `start` alone runs to the end of the blob.
        assert_eq!(blobs.get(&id, &opts(Some(7), None, None)).await?, b"hij");
        // `length` wins over `end` rather than conflicting with it.
        assert_eq!(
            blobs.get(&id, &opts(Some(0), Some(3), Some(9))).await?,
            b"abc"
        );
        Ok(())
    }

    /// Over-long and degenerate ranges clamp to the blob instead of reading into the next blob,
    /// which is what JS does. Verified against JS: with two 20 byte blobs at blockSize 10,
    /// `get(idA, {start: 0, length: 30})` there returns 20 bytes of `a` plus 10 bytes of `b`.
    #[tokio::test]
    async fn read_stream_clamps_to_the_blob() -> Result<()> {
        let blobs = ram_blobs(10).await;
        let id_a = blobs.put(&[b'a'; 20], &Default::default()).await?;
        blobs.put(&[b'b'; 20], &Default::default()).await?;

        for o in [
            opts(Some(0), Some(30), None),
            opts(Some(0), None, Some(29)),
            opts(None, Some(u64::MAX), None),
            opts(None, None, Some(u64::MAX)),
        ] {
            let got = blobs.get(&id_a, &o).await?;
            assert_eq!(
                got,
                vec![b'a'; 20],
                "over-long range escaped the blob: {o:?}"
            );
        }

        // Empty and inverted ranges yield nothing rather than erroring or overrunning.
        for o in [
            opts(Some(5), Some(0), None),
            opts(Some(20), None, None),
            opts(Some(100), None, None),
            opts(Some(10), None, Some(3)),
        ] {
            assert!(
                chunks(&blobs, &id_a, &o).await?.is_empty(),
                "expected no chunks for {o:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_stream_reports_a_missing_block_and_stops() -> Result<()> {
        let blobs = ram_blobs(4).await;
        let id = blobs.put(b"abcdefgh", &Default::default()).await?;

        // Point past everything the core holds, so the blocks cannot be read.
        let missing = GetBlob {
            block_offset: id.block_offset + 100,
            byte_length: 8,
        };
        let mut stream = std::pin::pin!(blobs.read_stream(&missing, &Default::default()));

        let first = stream.next().await.expect("a chunk is yielded");
        assert!(
            matches!(first, Err(Error::BlockNotAvailable(index)) if index == 100),
            "expected BlockNotAvailable, got {first:?}"
        );
        // The error terminates the stream rather than retrying the same block forever.
        assert!(stream.next().await.is_none());

        // A range starting past byte zero has to read the blob's first block in order to seek, so
        // it reports that block missing rather than the one it was aiming for.
        let res = blobs.get(&missing, &opts(Some(5), None, None)).await;
        assert!(
            matches!(res, Err(Error::BlockNotAvailable(100))),
            "expected BlockNotAvailable(100), got {res:?}"
        );
        Ok(())
    }

    /// A range read must not depend on the *reader's* `block_size`, only on the size the blob was
    /// actually written at. Both directions of mismatch used to error; now they read.
    #[tokio::test]
    async fn read_stream_ignores_the_readers_block_size() -> Result<()> {
        for (written_at, read_as) in [(8, 4), (4, 10), (3, 64), (64, 3)] {
            let writer = ram_blobs(written_at).await;
            let id = writer.put(b"abcdefgh", &Default::default()).await?;
            let reader = HyperblobsBuilder::default()
                .block_size(read_as)
                .core(writer.core)
                .build()
                .unwrap();

            let ctx = format!("written at {written_at}, read by a store set to {read_as}");
            assert_eq!(
                reader.get(&id, &Default::default()).await?,
                b"abcdefgh",
                "{ctx}"
            );
            assert_eq!(
                reader.get(&id, &opts(Some(4), None, None)).await?,
                b"efgh",
                "{ctx}"
            );
            assert_eq!(
                reader.get(&id, &opts(Some(5), None, None)).await?,
                b"fgh",
                "{ctx}"
            );
            assert_eq!(
                reader.get(&id, &opts(Some(1), Some(6), None)).await?,
                b"bcdefg",
                "{ctx}"
            );
        }
        Ok(())
    }

    /// The same store can hold blobs chunked at different sizes, and reads back each one at the
    /// size it was written at. This is JS's per-put `blockSize`.
    #[tokio::test]
    async fn put_honors_a_per_blob_block_size() -> Result<()> {
        let blobs = ram_blobs(10).await;
        let data = b"abcdefghijklmnopqrst"; // 20 bytes

        let fine = blobs
            .put(
                data,
                &PutOptions {
                    block_size: Some(4),
                },
            )
            .await?;
        assert_eq!(fine.block_length, 5);
        let coarse = blobs.put(data, &Default::default()).await?;
        assert_eq!(coarse.block_length, 2);
        // Zero means "unset", as in JS, rather than dividing by zero.
        let zero = blobs
            .put(
                data,
                &PutOptions {
                    block_size: Some(0),
                },
            )
            .await?;
        assert_eq!(zero.block_length, 2);

        for id in [&fine, &coarse, &zero] {
            assert_eq!(blobs.get(id, &Default::default()).await?, data);
            assert_eq!(blobs.get(id, &opts(Some(9), Some(4), None)).await?, b"jklm");
            assert_eq!(blobs.get(id, &opts(Some(19), None, None)).await?, b"t");
        }

        // The finely chunked blob really is stored in 4 byte blocks.
        assert_eq!(
            chunks(&blobs, &fine, &Default::default()).await?,
            vec![
                b"abcd".to_vec(),
                b"efgh".to_vec(),
                b"ijkl".to_vec(),
                b"mnop".to_vec(),
                b"qrst".to_vec()
            ]
        );
        Ok(())
    }

    /// Recovering the block size from the blob's first block assumes uniform chunking, which
    /// holds for anything `put` wrote but not for blocks appended by hand. A read that would make
    /// no progress has to error rather than spin.
    #[tokio::test]
    async fn read_stream_reports_a_short_block_and_stops() -> Result<()> {
        let blobs = ram_blobs(10).await;
        // Deliberately non-uniform: 10, then 2, then 8.
        blobs
            .core()
            .append_batch(&[vec![b'a'; 10], vec![b'b'; 2], vec![b'c'; 8]])
            .await?;
        let id = GetBlob {
            block_offset: 0,
            byte_length: 20,
        };

        // Byte 13 lands in block 1 at offset 3, but block 1 holds only 2 bytes.
        let mut stream = std::pin::pin!(blobs.read_stream(&id, &opts(Some(13), None, None)));
        let first = stream.next().await.expect("a chunk is yielded");
        assert!(
            matches!(first, Err(Error::ShortBlock(1))),
            "expected ShortBlock(1), got {first:?}"
        );
        assert!(stream.next().await.is_none());

        // An empty first block gives no block size to seek with.
        let blobs = ram_blobs(10).await;
        blobs.core().append_batch(&[vec![], vec![b'a'; 10]]).await?;
        let id = GetBlob {
            block_offset: 0,
            byte_length: 10,
        };
        let res = blobs.get(&id, &opts(Some(1), None, None)).await;
        assert!(
            matches!(res, Err(Error::ShortBlock(0))),
            "expected ShortBlock(0), got {res:?}"
        );
        Ok(())
    }
}
