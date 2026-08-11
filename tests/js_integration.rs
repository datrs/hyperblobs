//! Test [`hyperblobs`] against the JavaScript `hyperblobs` it is a port of.
//!
//! The two implementations do not replicate to each other here. They exchange the *raw hypercore
//! blocks* a blob is made of, plus the blob id, through the Node.js REPL. That is enough to pin
//! down everything the two have to agree on: how `put` chunks a blob into blocks, the
//! `{byteOffset, blockOffset, blockLength, byteLength}` id, and which bytes a `{start, length,
//! end}` range selects. Keeping replication out of it means a failure here is a blob-layout bug
//! and nothing else.
//!
//! JS `hyperblobs` is pinned to 2.8.0, the last release before it moved to hypercore 11. The Rust
//! tree tracks hypercore 10.

mod common;

use futures_lite::StreamExt;
use hyperblobs::{GetOptions, Hyperblobs, HyperblobsBuilder, PutBlob, PutOptions};
use hypercore::{HypercoreBuilder, Storage};
use rusty_nodejs_repl::{Config, Repl};

use common::{
    js::{path_to_node_modules, REQUIRE_JS},
    Result,
};

/// Small enough that the fixture blobs span several blocks, so ranges land on and between block
/// boundaries.
const BLOCK_SIZE: u64 = 10;

/// The blobs both implementations write, in order. Between them they cover a blob of whole
/// blocks, a blob with a partial final block, and an empty blob (which occupies no blocks at
/// all).
///
/// The 30 bytes of `c` after the `a`/`b` blob are load bearing: they let
/// [`ranges_past_the_end_of_a_blob_diverge_from_js`] read past the end of that blob without
/// running off the end of the core, which JS never recovers from.
fn fixture() -> Vec<Vec<u8>> {
    vec![
        vec![b'z'; 50],
        [vec![b'a'; 10], vec![b'b'; 10]].concat(),
        vec![b'c'; 30],
        b"1234567".to_vec(),
        vec![],
    ]
}

/// Index into [`fixture`] of the 20 byte blob the range matrix is built around.
const AB: usize = 1;
/// Index into [`fixture`] of the 7 byte blob, which is smaller than one block.
const SUB_BLOCK: usize = 3;

fn opts(start: Option<u64>, length: Option<u64>, end: Option<u64>) -> GetOptions {
    GetOptions {
        start,
        length,
        end,
        ..Default::default()
    }
}

/// Ranges within the 20 byte `a`/`b` blob, which is exactly two blocks.
fn ab_ranges() -> Vec<GetOptions> {
    vec![
        opts(None, None, None),
        opts(Some(0), None, None),
        // Starts mid-block and crosses the boundary into the next one.
        opts(Some(8), Some(4), None),
        // Starts exactly on the second block.
        opts(Some(10), None, None),
        // `end` is inclusive.
        opts(Some(2), None, Some(4)),
        // `length` wins over `end` rather than conflicting with it.
        opts(Some(0), Some(3), Some(9)),
        opts(Some(19), None, None),
        opts(Some(0), Some(20), None),
        opts(Some(0), None, Some(19)),
        opts(Some(5), Some(10), None),
        // Degenerate ranges: empty, starting at the end, past the end, and inverted.
        opts(Some(0), Some(0), None),
        opts(Some(20), None, None),
        opts(Some(21), None, None),
        opts(Some(10), None, Some(3)),
    ]
}

/// Ranges within the 7 byte blob, all of which stay inside a single block.
fn sub_block_ranges() -> Vec<GetOptions> {
    vec![
        opts(None, None, None),
        opts(Some(2), Some(3), None),
        opts(Some(0), None, Some(6)),
        opts(Some(6), None, None),
    ]
}

// ---------------------------------------------------------------- JavaScript

/// Start Node.js with an empty in-memory `hyperblobs` in scope as `blobs`, over a `hypercore` in
/// scope as `core`.
async fn start_repl() -> Result<Repl> {
    let _ = &*REQUIRE_JS;
    let mut conf = Config::build()?;
    conf.imports.push(
        "
Hypercore = require('hypercore');
Hyperblobs = require('hyperblobs');
RAM = require('random-access-memory');
"
        .to_string(),
    );
    conf.before.push(format!(
        "
core = new Hypercore(RAM);
await core.ready();
blobs = new Hyperblobs(core, {{ blockSize: {BLOCK_SIZE} }});
"
    ));
    conf.after.push("await core.close();".to_string());
    conf.path_to_node_modules = Some(path_to_node_modules()?.display().to_string());
    Ok(conf.start().await?)
}

/// Format bytes as a JS `Buffer.from([..])` literal.
fn js_buffer(bytes: &[u8]) -> String {
    let nums: Vec<String> = bytes.iter().map(|b| b.to_string()).collect();
    format!("Buffer.from([{}])", nums.join(","))
}

/// Format a [`PutBlob`] as the object literal JS uses for a blob id. [`PutBlob`] serializes
/// `camelCase`, which is exactly that shape, so this doubles as a check that the two agree on the
/// serialized form of an id.
fn js_id(id: &PutBlob) -> String {
    serde_json::to_string(id).expect("a PutBlob serializes")
}

/// Format [`GetOptions`] as the JS options object. `timeout` is left out because Rust does not
/// honor it yet, so there would be nothing to compare.
fn js_opts(opts: &GetOptions) -> String {
    let mut fields = vec![];
    if let Some(start) = opts.start {
        fields.push(format!("start:{start}"));
    }
    if let Some(length) = opts.length {
        fields.push(format!("length:{length}"));
    }
    if let Some(end) = opts.end {
        fields.push(format!("end:{end}"));
    }
    format!("{{{}}}", fields.join(","))
}

/// Format [`PutOptions`] as the JS options object.
fn js_put_opts(opts: &PutOptions) -> String {
    match opts.block_size {
        Some(block_size) => format!("{{blockSize:{block_size}}}"),
        None => "{}".to_string(),
    }
}

async fn js_put(repl: &mut Repl, blob: &[u8], opts: &PutOptions) -> Result<PutBlob> {
    Ok(repl
        .json_run(format!(
            "outputJson(await blobs.put({}, {}))",
            js_buffer(blob),
            js_put_opts(opts)
        ))
        .await?)
}

async fn js_get(repl: &mut Repl, id: &PutBlob, opts: &GetOptions) -> Result<Vec<u8>> {
    Ok(repl
        .json_run(format!(
            "outputJson([...(await blobs.get({}, {}))])",
            js_id(id),
            js_opts(opts)
        ))
        .await?)
}

/// The chunks JS's `createReadStream` emits, which is what [`Hyperblobs::read_stream`] is a port
/// of. Comparing chunk by chunk rather than concatenated is the point: it pins the chunk
/// boundaries, not just the bytes.
async fn js_read_stream(repl: &mut Repl, id: &PutBlob, opts: &GetOptions) -> Result<Vec<Vec<u8>>> {
    Ok(repl
        .json_run(format!(
            "
_chunks = [];
for await (const c of blobs.createReadStream({}, {})) _chunks.push([...c]);
outputJson(_chunks);
",
            js_id(id),
            js_opts(opts)
        ))
        .await?)
}

/// Every raw block in the JS core, in order.
async fn js_core_blocks(repl: &mut Repl) -> Result<Vec<Vec<u8>>> {
    Ok(repl
        .json_run(
            "
_blocks = [];
for (let i = 0; i < core.length; i++) _blocks.push([...(await core.get(i))]);
outputJson(_blocks);
",
        )
        .await?)
}

/// Append raw blocks to the JS core, so JS can read blobs Rust wrote.
async fn js_append_blocks(repl: &mut Repl, blocks: &[Vec<u8>]) -> Result<()> {
    let list: Vec<String> = blocks.iter().map(|b| js_buffer(b)).collect();
    repl.run(format!("await core.append([{}]);", list.join(",")))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------- Rust

async fn ram_blobs() -> Result<Hyperblobs> {
    let core = HypercoreBuilder::new(Storage::new_memory().await?)
        .build()
        .await?;
    Ok(HyperblobsBuilder::default()
        .block_size(BLOCK_SIZE)
        .core(core)
        .build()?)
}

/// Every raw block in the Rust core, in order.
async fn rust_core_blocks(blobs: &Hyperblobs) -> Result<Vec<Vec<u8>>> {
    let mut out = vec![];
    let mut index = 0;
    while let Some(block) = blobs.core().get(index).await? {
        out.push(block);
        index += 1;
    }
    Ok(out)
}

async fn rust_read_stream(
    blobs: &Hyperblobs,
    id: &PutBlob,
    opts: &GetOptions,
) -> Result<Vec<Vec<u8>>> {
    let mut stream = std::pin::pin!(blobs.read_stream(id, opts));
    let mut out = vec![];
    while let Some(chunk) = stream.next().await {
        out.push(chunk?);
    }
    Ok(out)
}

// ------------------------------------------------------------------ fixtures

/// Write [`fixture`] with JS, then hand the raw blocks it produced to a Rust [`Hyperblobs`], so
/// Rust reads bytes JS laid out.
async fn js_writes_rust_reads(repl: &mut Repl) -> Result<(Hyperblobs, Vec<PutBlob>)> {
    let mut ids = vec![];
    for blob in fixture() {
        ids.push(js_put(repl, &blob, &Default::default()).await?);
    }
    let blobs = ram_blobs().await?;
    blobs
        .core()
        .append_batch(&js_core_blocks(repl).await?)
        .await?;
    Ok((blobs, ids))
}

/// Write [`fixture`] with Rust, then hand the raw blocks it produced to the JS core, so JS reads
/// bytes Rust laid out.
async fn rust_writes_js_reads(repl: &mut Repl) -> Result<(Hyperblobs, Vec<PutBlob>)> {
    let blobs = ram_blobs().await?;
    let mut ids = vec![];
    for blob in fixture() {
        ids.push(blobs.put(&blob, &Default::default()).await?);
    }
    js_append_blocks(repl, &rust_core_blocks(&blobs).await?).await?;
    Ok((blobs, ids))
}

// --------------------------------------------------------------------- tests

/// Both implementations must chunk a blob into the same blocks and describe it with the same id,
/// or nothing else in this file means anything.
#[tokio::test]
async fn rust_and_js_put_the_same_blocks_and_ids() -> Result<()> {
    let mut repl = start_repl().await?;
    let blobs = ram_blobs().await?;

    for blob in fixture() {
        let js = js_put(&mut repl, &blob, &Default::default()).await?;
        let rs = blobs.put(&blob, &Default::default()).await?;
        // Compared as JSON so the `camelCase` field names are part of the assertion.
        assert_eq!(
            serde_json::to_value(&rs)?,
            serde_json::to_value(&js)?,
            "id mismatch for a {} byte blob",
            blob.len()
        );
    }

    assert_eq!(
        rust_core_blocks(&blobs).await?,
        js_core_blocks(&mut repl).await?
    );

    repl.stop().await?;
    Ok(())
}

/// Rust reads blobs JS wrote, over a matrix of ranges. This is the direction that matters for
/// reading somebody else's feed.
#[tokio::test]
async fn rust_reads_the_ranges_js_wrote() -> Result<()> {
    let mut repl = start_repl().await?;
    let (blobs, ids) = js_writes_rust_reads(&mut repl).await?;

    for (i, ranges) in [(AB, ab_ranges()), (SUB_BLOCK, sub_block_ranges())] {
        for o in ranges {
            let js = js_get(&mut repl, &ids[i], &o).await?;
            let rs = blobs.get(&ids[i], &o).await?;
            assert_eq!(rs, js, "blob {i} read with {o:?}");
        }
    }

    // Every blob in the fixture reads back whole, including the empty one and the one with a
    // partial final block.
    for (i, blob) in fixture().iter().enumerate() {
        let rs = blobs.get(&ids[i], &Default::default()).await?;
        assert_eq!(&rs, blob, "blob {i} did not read back whole");
    }

    repl.stop().await?;
    Ok(())
}

/// JS reads blobs Rust wrote, over the same matrix. The direction that matters for publishing a
/// feed a JS client will consume.
#[tokio::test]
async fn js_reads_the_ranges_rust_wrote() -> Result<()> {
    let mut repl = start_repl().await?;
    let (blobs, ids) = rust_writes_js_reads(&mut repl).await?;

    for (i, ranges) in [(AB, ab_ranges()), (SUB_BLOCK, sub_block_ranges())] {
        for o in ranges {
            let js = js_get(&mut repl, &ids[i], &o).await?;
            let rs = blobs.get(&ids[i], &o).await?;
            assert_eq!(js, rs, "blob {i} read with {o:?}");
        }
    }

    repl.stop().await?;
    Ok(())
}

/// `read_stream` is a port of JS's `createReadStream`, so it has to emit the same chunks, not
/// merely the same concatenated bytes. Getting the boundaries right is the whole point of
/// streaming a blob instead of buffering it.
#[tokio::test]
async fn read_stream_emits_the_same_chunks_as_js() -> Result<()> {
    let mut repl = start_repl().await?;
    let (blobs, ids) = js_writes_rust_reads(&mut repl).await?;

    for (i, ranges) in [(AB, ab_ranges()), (SUB_BLOCK, sub_block_ranges())] {
        for o in ranges {
            let js = js_read_stream(&mut repl, &ids[i], &o).await?;
            let rs = rust_read_stream(&blobs, &ids[i], &o).await?;
            assert_eq!(rs, js, "blob {i} streamed with {o:?}");
        }
    }

    // One chunk per block when reading a whole blob, for every blob in the fixture.
    for (i, _) in fixture().iter().enumerate() {
        let o = Default::default();
        let js = js_read_stream(&mut repl, &ids[i], &o).await?;
        let rs = rust_read_stream(&blobs, &ids[i], &o).await?;
        assert_eq!(rs, js, "blob {i} streamed whole");
    }

    repl.stop().await?;
    Ok(())
}

/// JS's per-put `blockSize`, which [`PutOptions::block_size`] is the port of. One store holds
/// blobs chunked at a mix of sizes, and each reads back at the size it was written at rather than
/// at the store's own.
///
/// This is the case that made `read_stream` stop consulting the store's `block_size` and recover
/// the blob's own instead. Before that, the reads below with a non-zero `start` landed on the
/// wrong block of the finely chunked blob and errored.
#[tokio::test]
async fn per_blob_block_size_round_trips_with_js() -> Result<()> {
    let mut repl = start_repl().await?;
    let blobs = ram_blobs().await?;

    // 20 bytes chunked at 4, in a store whose own block size is 10.
    let data: Vec<u8> = (b'a'..=b't').collect();
    let fine = PutOptions {
        block_size: Some(4),
    };

    let js_fine = js_put(&mut repl, &data, &fine).await?;
    let rs_fine = blobs.put(&data, &fine).await?;
    assert_eq!(
        rs_fine.block_length, 5,
        "the store's block size of {BLOCK_SIZE} was used instead of the per-blob one"
    );
    assert_eq!(
        serde_json::to_value(&rs_fine)?,
        serde_json::to_value(&js_fine)?
    );

    // A second blob at the store's own size, so the two sit back to back at different sizes and
    // the finely chunked one is no longer the only thing in the core.
    let js_coarse = js_put(&mut repl, &data, &Default::default()).await?;
    let rs_coarse = blobs.put(&data, &Default::default()).await?;
    assert_eq!(rs_coarse.block_length, 2);
    assert_eq!(
        serde_json::to_value(&rs_coarse)?,
        serde_json::to_value(&js_coarse)?
    );

    assert_eq!(
        rust_core_blocks(&blobs).await?,
        js_core_blocks(&mut repl).await?
    );

    for (rs_id, js_id) in [(&rs_fine, &js_fine), (&rs_coarse, &js_coarse)] {
        for o in [
            opts(None, None, None),
            opts(Some(9), Some(4), None),
            opts(Some(4), None, None),
            opts(Some(19), None, None),
            opts(Some(2), None, Some(17)),
        ] {
            assert_eq!(
                blobs.get(rs_id, &o).await?,
                js_get(&mut repl, js_id, &o).await?,
                "blob of {} blocks read with {o:?}",
                rs_id.block_length
            );
            assert_eq!(
                rust_read_stream(&blobs, rs_id, &o).await?,
                js_read_stream(&mut repl, js_id, &o).await?,
                "blob of {} blocks streamed with {o:?}",
                rs_id.block_length
            );
        }
    }

    repl.stop().await?;
    Ok(())
}

/// The one place the port deliberately disagrees with JS.
///
/// A `length`/`end` that runs past the end of the blob reads on into whatever blob comes next in
/// JS, because a read stream there is bounded by the range and not by the blob. Rust clamps the
/// range to the blob instead: [`hyperblobs::GetBlob`] carries no `byteOffset`/`blockLength` with
/// which to describe anything outside the blob, and overrunning is not a useful thing to offer.
///
/// JS's behaviour is worse than it looks. Overrun past the end of the *core* rather than into a
/// following blob and the read simply never resolves, which is why this test keeps the overrun
/// inside the trailing `c` blob.
#[tokio::test]
async fn ranges_past_the_end_of_a_blob_diverge_from_js() -> Result<()> {
    let mut repl = start_repl().await?;
    let (blobs, ids) = js_writes_rust_reads(&mut repl).await?;
    let id = &ids[AB];

    let whole = fixture()[AB].clone();
    let overrun = [whole.clone(), vec![b'c'; 10]].concat();

    for o in [opts(Some(0), Some(30), None), opts(Some(0), None, Some(29))] {
        assert_eq!(
            js_get(&mut repl, id, &o).await?,
            overrun,
            "expected JS to read on into the next blob with {o:?}"
        );
        assert_eq!(
            blobs.get(id, &o).await?,
            whole,
            "expected Rust to clamp to the blob with {o:?}"
        );
    }

    repl.stop().await?;
    Ok(())
}
