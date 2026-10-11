// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Host-boundary baseline for issue #976.
//!
//! Runs the same five cases twice: `engine` calls `HawDBEmbedded`
//! directly (no host conversion, the lower bound) and `ffi` calls the
//! exported `hawdb_query`/`hawdb_query_sql` functions (engine plus
//! JSON serialization plus the buffer copy, without a host decode).
//! The difference between `ffi` and `engine` is the Rust-side boundary
//! cost; the Go benchmark reports the remaining host-side share against
//! the same checksum, so value parity is verifiable across languages.
//!
//! Usage: `cargo bench --bench boundary` (release) or
//! `cargo run --release --bench boundary`. Sizes come from
//! `HAWDB_BENCH_SIZES` (comma-separated, default `1000`) and backends
//! from `HAWDB_BENCH_BACKENDS` (comma-separated `memory,file`, default
//! `memory`).
//!
//! Output is one CSV-ish line per measurement:
//! `bench,layer=..,backend=..,case=..,n=..,wall_ms=..,ops=..,rows_out=..,values_out=..,checksum=..`

use hawdb::{HawDBEmbedded, HawDBEmbeddedOpenOptions, QueryOutput, Value};
use hawdb_ffi::{
    hawdb_buffer_free, hawdb_close, hawdb_open, hawdb_query, hawdb_query_ex, hawdb_query_sql,
    HawdbBuffer, HawdbDatabase, HawdbQueryRequest, HAWDB_DIALECT_CYPHER, HAWDB_DIALECT_SQL,
    HAWDB_FORMAT_BINARY, HAWDB_FORMAT_JSON,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::ffi::{c_char, CString};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Peak RSS of this process in KiB (`ru_maxrss`, bytes on macOS and
/// KiB on Linux).
#[cfg(unix)]
fn peak_rss_kb() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    #[cfg(target_os = "macos")]
    let bytes = usage.ru_maxrss;
    #[cfg(not(target_os = "macos"))]
    let bytes = usage.ru_maxrss * 1024;
    bytes / 1024
}

#[cfg(not(unix))]
fn peak_rss_kb() -> i64 {
    0
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn hash_len(hash: &mut u64, len: usize) {
    hash_bytes(hash, &(len as u64).to_le_bytes());
}

/// Canonical value hash shared with the Go benchmark: a type tag plus a
/// fixed little-endian/UTF-8 payload. `Map` iterates `BTreeMap` order,
/// which Go reproduces by sorting keys.
fn hash_value(hash: &mut u64, value: &Value) {
    match value {
        Value::Null => hash_bytes(hash, &[0]),
        Value::Bool(item) => hash_bytes(hash, &[1, u8::from(*item)]),
        Value::Int(item) => {
            hash_bytes(hash, &[2]);
            hash_bytes(hash, &item.to_le_bytes());
        }
        Value::Float(item) => {
            hash_bytes(hash, &[3]);
            hash_bytes(hash, &item.to_bits().to_le_bytes());
        }
        Value::String(item) => {
            hash_bytes(hash, &[4]);
            hash_len(hash, item.len());
            hash_bytes(hash, item.as_bytes());
        }
        Value::Binary(item) => {
            hash_bytes(hash, &[5]);
            hash_len(hash, item.len());
            hash_bytes(hash, item);
        }
        Value::Uuid(item) => {
            hash_bytes(hash, &[6]);
            hash_bytes(hash, item.to_string().as_bytes());
        }
        Value::List(items) => {
            hash_bytes(hash, &[7]);
            hash_len(hash, items.len());
            for item in items {
                hash_value(hash, item);
            }
        }
        Value::Map(items) => {
            hash_bytes(hash, &[8]);
            hash_len(hash, items.len());
            for (key, item) in items {
                hash_len(hash, key.len());
                hash_bytes(hash, key.as_bytes());
                hash_value(hash, item);
            }
        }
    }
}

/// The same canonical hash over the JSON result envelope, matching the
/// tagged `$binary`/`$uuid` decoding a host performs.
fn hash_json_value(hash: &mut u64, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => hash_bytes(hash, &[0]),
        serde_json::Value::Bool(item) => hash_bytes(hash, &[1, u8::from(*item)]),
        serde_json::Value::Number(item) => {
            if let Some(item) = item.as_i64() {
                hash_bytes(hash, &[2]);
                hash_bytes(hash, &item.to_le_bytes());
            } else {
                let item = item.as_f64().unwrap_or(0.0);
                hash_bytes(hash, &[3]);
                hash_bytes(hash, &item.to_bits().to_le_bytes());
            }
        }
        serde_json::Value::String(item) => {
            hash_bytes(hash, &[4]);
            hash_len(hash, item.len());
            hash_bytes(hash, item.as_bytes());
        }
        serde_json::Value::Array(items) => {
            hash_bytes(hash, &[7]);
            hash_len(hash, items.len());
            for item in items {
                hash_json_value(hash, item);
            }
        }
        serde_json::Value::Object(items) => {
            if items.len() == 1 {
                if let Some(encoded) = items.get("$binary").and_then(|v| v.as_str()) {
                    let decoded =
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                            .expect("bench data uses valid base64");
                    hash_bytes(hash, &[5]);
                    hash_len(hash, decoded.len());
                    hash_bytes(hash, &decoded);
                    return;
                }
                if let Some(encoded) = items.get("$uuid").and_then(|v| v.as_str()) {
                    hash_bytes(hash, &[6]);
                    hash_bytes(hash, encoded.as_bytes());
                    return;
                }
            }
            hash_bytes(hash, &[8]);
            hash_len(hash, items.len());
            for (key, item) in items {
                hash_len(hash, key.len());
                hash_bytes(hash, key.as_bytes());
                hash_json_value(hash, item);
            }
        }
    }
}

/// Serialize a `QueryOutput` to the same envelope the FFI layer emits,
/// for byte-level parity debugging (`HAWDB_BENCH_DUMP=<dir>`).
fn output_to_envelope_json(output: &QueryOutput) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = output
        .value_rows()
        .map(|row| serde_json::Value::Array(row.iter().map(FfiDriver::value_json).collect()))
        .collect();
    serde_json::json!({
        "columns": output.schema().columns(),
        "rows": rows,
    })
}

fn hash_output(output: &QueryOutput) -> (u64, u64, u64) {
    let mut hash = FNV_OFFSET;
    let mut rows = 0u64;
    let mut values = 0u64;
    for row in output.value_rows() {
        rows += 1;
        values += row.len() as u64;
        for value in row {
            hash_value(&mut hash, value);
        }
    }
    (hash, rows, values)
}

fn hash_result_envelope(json_bytes: &[u8]) -> (u64, u64, u64) {
    let envelope: serde_json::Value =
        serde_json::from_slice(json_bytes).expect("ffi result is valid JSON");
    let mut hash = FNV_OFFSET;
    let mut rows = 0u64;
    let mut values = 0u64;
    if let Some(rows_json) = envelope.get("rows").and_then(|v| v.as_array()) {
        for row in rows_json {
            rows += 1;
            if let Some(items) = row.as_array() {
                values += items.len() as u64;
                for value in items {
                    hash_json_value(&mut hash, value);
                }
            }
        }
    }
    (hash, rows, values)
}

// ---------------------------------------------------------------------
// Binary envelope reader: mirrors the encoder in src/lib.rs so the
// ffi_bin layer's checksum is computed over decoded Values, keeping
// parity with the engine and JSON layers honest.
struct BinReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl BinReader<'_> {
    fn take(&mut self, n: usize) -> &[u8] {
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        slice
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take(4).try_into().unwrap())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }
    fn i64(&mut self) -> i64 {
        i64::from_le_bytes(self.take(8).try_into().unwrap())
    }
    fn f64(&mut self) -> f64 {
        f64::from_le_bytes(self.take(8).try_into().unwrap())
    }
}

fn read_bin_value(r: &mut BinReader) -> Value {
    match r.u8() {
        0 => Value::Null,
        1 => Value::Bool(r.u8() != 0),
        2 => Value::Int(r.i64()),
        3 => Value::Float(r.f64()),
        4 => {
            let len = r.u64() as usize;
            Value::String(String::from_utf8(r.take(len).to_vec()).unwrap())
        }
        5 => {
            let len = r.u64() as usize;
            Value::Binary(r.take(len).to_vec())
        }
        6 => Value::Uuid(hawdb::Uuid::from_bytes(r.take(16).try_into().unwrap())),
        7 => {
            let n = r.u64();
            Value::List((0..n).map(|_| read_bin_value(r)).collect())
        }
        8 => {
            let n = r.u64();
            let mut map = BTreeMap::new();
            for _ in 0..n {
                let klen = r.u32() as usize;
                let key = String::from_utf8(r.take(klen).to_vec()).unwrap();
                map.insert(key, read_bin_value(r));
            }
            Value::Map(map)
        }
        tag => panic!("unknown binary value tag {tag}"),
    }
}

fn hash_binary_envelope(bytes: &[u8]) -> (u64, u64, u64) {
    let mut r = BinReader { buf: bytes, pos: 0 };
    assert_eq!(r.take(4), b"HDB1", "bad binary envelope magic");
    let columns = r.u32();
    for _ in 0..columns {
        let len = r.u32();
        r.take(len as usize);
    }
    let row_count = r.u64();
    let mut hash = FNV_OFFSET;
    let mut values = 0u64;
    for _ in 0..row_count {
        for _ in 0..columns {
            hash_value(&mut hash, &read_bin_value(&mut r));
            values += 1;
        }
    }
    (hash, row_count, values)
}

// ---------------------------------------------------------------------
// Deterministic dataset: values derived from the row id through a LCG,
// so every layer/language generates byte-identical inputs.

struct Generator {
    state: u64,
}

impl Generator {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state >> 11
    }

    fn string(&mut self, len: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz012345";
        (0..len)
            .map(|_| ALPHABET[(self.next() % 32) as usize] as char)
            .collect()
    }

    /// One `(id, v, s)` row for the SQL cases.
    fn row3(&mut self, id: i64) -> (i64, i64, String) {
        (id, self.next() as i64, self.string(16))
    }

    /// One `Wide` node map for the nested case: id, 8 ints, 5 floats,
    /// 4 strings, a 5-int list and a 3-key map — 20 properties.
    fn wide_props(&mut self, id: i64) -> Vec<(String, Value)> {
        let mut props = vec![("id".to_string(), Value::Int(id))];
        for i in 0..8 {
            props.push((format!("i{i}"), Value::Int(self.next() as i64)));
        }
        for i in 0..5 {
            props.push((
                format!("f{i}"),
                Value::Float((self.next() % 100_000) as f64 / 7.0),
            ));
        }
        for i in 0..4 {
            props.push((format!("s{i}"), Value::String(self.string(12))));
        }
        props.push((
            "lst".to_string(),
            Value::List(
                (0..5)
                    .map(|_| Value::Int(self.next() as i64 % 1_000))
                    .collect(),
            ),
        ));
        props.push((
            "mp".to_string(),
            Value::Map(BTreeMap::from([
                ("k1".to_string(), Value::Int(self.next() as i64)),
                ("k2".to_string(), Value::String(self.string(8))),
                ("k3".to_string(), Value::Bool(self.next().is_multiple_of(2))),
            ])),
        ));
        props
    }
}

fn escape_sql(text: &str) -> String {
    text.replace('\'', "''")
}

// ---------------------------------------------------------------------
// Layer drivers.

trait Driver {
    fn open(backend: &Backend, tag: &str) -> Self;
    fn sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryOutcome, String>;
    fn cypher(
        &mut self,
        cypher: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryOutcome, String>;
    fn close(self);
}

struct QueryOutcome {
    checksum: u64,
    rows: u64,
    values: u64,
}

enum Backend {
    Memory,
    File(PathBuf),
}

impl Backend {
    fn open_engine(&self, tag: &str) -> HawDBEmbedded {
        match self {
            Backend::Memory => HawDBEmbedded::open_in_memory(),
            Backend::File(dir) => HawDBEmbedded::open_with_options(HawDBEmbeddedOpenOptions::new(
                dir.join(format!("{tag}.hawdb")),
            ))
            .expect("open file database"),
        }
    }

    fn open_ffi(&self, tag: &str) -> *mut HawdbDatabase {
        let (path, options) = match self {
            Backend::Memory => (
                CString::new(":memory:").unwrap(),
                CString::new(json!({"memory": true}).to_string()).unwrap(),
            ),
            Backend::File(dir) => (
                CString::new(
                    dir.join(format!("{tag}.hawdb"))
                        .to_string_lossy()
                        .into_owned(),
                )
                .unwrap(),
                CString::new("{}").unwrap(),
            ),
        };
        let mut err = HawdbBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let db = unsafe {
            hawdb_open(
                path.as_ptr() as *const c_char,
                path.as_bytes().len(),
                options.as_ptr() as *const c_char,
                options.as_bytes().len(),
                &mut err,
            )
        };
        assert!(!db.is_null(), "hawdb_open failed");
        db
    }
}

fn take_buffer(buffer: &mut HawdbBuffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        return Vec::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr as *const u8, buffer.len) }.to_vec();
    unsafe { hawdb_buffer_free(buffer) };
    bytes
}

fn maybe_dump(layer: &str, tag: &str, bytes: &[u8]) {
    if let Ok(dir) = env::var("HAWDB_BENCH_DUMP") {
        let ext = if layer == "ffi_bin" { "bin" } else { "json" };
        std::fs::create_dir_all(&dir).ok();
        std::fs::write(format!("{dir}/{tag}_{layer}.{ext}"), bytes).ok();
    }
}

struct EngineDriver {
    db: HawDBEmbedded,
    tag: String,
}

impl Driver for EngineDriver {
    fn open(backend: &Backend, tag: &str) -> Self {
        Self {
            // Per-layer file names so a second layer does not reopen the
            // first layer's populated database.
            db: backend.open_engine(&format!("{tag}_engine")),
            tag: tag.to_string(),
        }
    }

    fn sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryOutcome, String> {
        let output = self
            .db
            .database_mut()
            .query_sql_with_params(sql, params)
            .map_err(|error| error.to_string())?;
        let (checksum, rows, values) = hash_output(&output);
        black_box(&output);
        maybe_dump(
            "engine",
            &self.tag,
            &output_to_envelope_json(&output).to_string().into_bytes(),
        );
        Ok(QueryOutcome {
            checksum,
            rows,
            values,
        })
    }

    fn cypher(
        &mut self,
        cypher: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryOutcome, String> {
        let output = self
            .db
            .query_with_params_admitted(cypher, params)
            .map_err(|error| error.to_string())?;
        let (checksum, rows, values) = hash_output(&output);
        black_box(&output);
        maybe_dump(
            "engine",
            &self.tag,
            &output_to_envelope_json(&output).to_string().into_bytes(),
        );
        Ok(QueryOutcome {
            checksum,
            rows,
            values,
        })
    }

    fn close(self) {
        drop(self.db);
    }
}

struct FfiDriver {
    db: *mut HawdbDatabase,
    tag: String,
    binary: bool,
}

impl FfiDriver {
    fn call(
        &mut self,
        sql_mode: bool,
        text: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<QueryOutcome, String> {
        let text_c = CString::new(text).unwrap();
        let params_c = CString::new(params.map(|p| p.to_string()).unwrap_or_default()).unwrap();
        let params_ptr = if params.is_some() {
            params_c.as_ptr() as *const c_char
        } else {
            std::ptr::null()
        };
        let params_len = params.map(|p| p.to_string().len()).unwrap_or(0);
        let mut result = HawdbBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let mut err = HawdbBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let (dialect, format) = (
            if sql_mode {
                HAWDB_DIALECT_SQL
            } else {
                HAWDB_DIALECT_CYPHER
            },
            if self.binary {
                HAWDB_FORMAT_BINARY
            } else {
                HAWDB_FORMAT_JSON
            },
        );
        let ok = unsafe {
            if self.binary {
                let request = HawdbQueryRequest {
                    struct_size: std::mem::size_of::<HawdbQueryRequest>(),
                    dialect,
                    text: text_c.as_ptr() as *const c_char,
                    text_len: text.len(),
                    params_json: params_ptr,
                    params_len,
                    format,
                };
                hawdb_query_ex(self.db, &request, &mut result, &mut err)
            } else {
                let entry = if sql_mode { hawdb_query_sql } else { hawdb_query };
                entry(
                    self.db,
                    text_c.as_ptr() as *const c_char,
                    text.len(),
                    params_ptr,
                    params_len,
                    &mut result,
                    &mut err,
                )
            }
        };
        if !ok {
            let err_bytes = take_buffer(&mut err);
            return Err(String::from_utf8_lossy(&err_bytes).into_owned());
        }
        if self.binary {
            let bytes = take_buffer(&mut result);
            black_box(&bytes);
            maybe_dump("ffi_bin", &self.tag, &bytes);
            let (checksum, rows, values) = hash_binary_envelope(&bytes);
            return Ok(QueryOutcome {
                checksum,
                rows,
                values,
            });
        }
        let bytes = take_buffer(&mut result);
        black_box(&bytes);
        maybe_dump("ffi", &self.tag, &bytes);
        let (checksum, rows, values) = hash_result_envelope(&bytes);
        Ok(QueryOutcome {
            checksum,
            rows,
            values,
        })
    }

    /// Mirror of the Go `prepareValue` tagging: params arrive with
    /// `$binary`/`$uuid` already tagged; bench values are scalars/lists/
    /// maps so `serde_json` conversion suffices.
    fn value_json(value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(v) => json!(v),
            Value::Int(v) => json!(v),
            Value::Float(v) => json!(v),
            Value::String(v) => json!(v),
            Value::Binary(v) => json!({ "$binary": base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD, v) }),
            Value::Uuid(v) => json!({ "$uuid": v.to_string() }),
            Value::List(items) => {
                serde_json::Value::Array(items.iter().map(Self::value_json).collect())
            }
            Value::Map(items) => serde_json::Value::Object(
                items
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::value_json(v)))
                    .collect(),
            ),
        }
    }
}

impl Driver for FfiDriver {
    fn open(backend: &Backend, tag: &str) -> Self {
        Self {
            db: backend.open_ffi(&format!("{tag}_ffi")),
            tag: tag.to_string(),
            binary: false,
        }
    }

    fn sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryOutcome, String> {
        let params_json = serde_json::Value::Array(params.iter().map(Self::value_json).collect());
        self.call(true, sql, Some(&params_json))
    }

    fn cypher(
        &mut self,
        cypher: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryOutcome, String> {
        let params_json = serde_json::Value::Object(
            params
                .iter()
                .map(|(k, v)| (k.clone(), Self::value_json(v)))
                .collect(),
        );
        self.call(false, cypher, Some(&params_json))
    }

    fn close(self) {
        unsafe { hawdb_close(self.db) };
    }
}

/// The FFI surface with the binary result envelope — same calls and
/// parameter encoding, different result wire format and free function.
struct FfiBinDriver(FfiDriver);

impl Driver for FfiBinDriver {
    fn open(backend: &Backend, tag: &str) -> Self {
        // Own file name so the binary and JSON FFI layers do not reopen
        // each other's populated database on the file backend.
        Self(FfiDriver {
            db: backend.open_ffi(&format!("{tag}_ffi_bin")),
            tag: tag.to_string(),
            binary: true,
        })
    }
    fn sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryOutcome, String> {
        self.0.sql(sql, params)
    }
    fn cypher(
        &mut self,
        cypher: &str,
        params: &BTreeMap<String, Value>,
    ) -> Result<QueryOutcome, String> {
        self.0.cypher(cypher, params)
    }
    fn close(self) {
        self.0.close();
    }
}

// ---------------------------------------------------------------------
// Cases.

const SELECT_COLS: &str = "SELECT id, v, s FROM bench_t";

fn setup_table(driver: &mut impl Driver) {
    driver
        .sql(
            "CREATE TABLE bench_t (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)",
            &[],
        )
        .expect("create table");
}

/// Identity of one measurement: which layer, backend, case and size it
/// belongs to.
struct Ctx<'a> {
    layer: &'a str,
    backend: &'a str,
    case: &'a str,
    n: u64,
}

/// Report a measured call; a failed statement records the error as the
/// measurement — budget denials are part of the contract being measured.
fn report_or_error(
    ctx: &Ctx,
    wall: std::time::Duration,
    ops: u64,
    outcome: Result<QueryOutcome, String>,
    rss_start_kb: i64,
) {
    match outcome {
        Ok(outcome) => report(ctx, wall, ops, outcome, rss_start_kb),
        Err(error) => {
            let Ctx {
                layer,
                backend,
                case,
                n,
            } = ctx;
            println!(
                "bench,layer={layer},backend={backend},case={case},n={n},wall_ms={:.3},ops={ops},status=error,peak_rss_delta_kb={},error={error:?}",
                wall.as_secs_f64() * 1e3,
                peak_rss_kb() - rss_start_kb,
            )
        }
    }
}

fn report(
    ctx: &Ctx,
    wall: std::time::Duration,
    ops: u64,
    outcome: QueryOutcome,
    rss_start_kb: i64,
) {
    let Ctx {
        layer,
        backend,
        case,
        n,
    } = ctx;
    println!(
        "bench,layer={layer},backend={backend},case={case},n={n},wall_ms={:.3},ops={ops},rows_out={},values_out={},checksum={:016x},peak_rss_delta_kb={}",
        wall.as_secs_f64() * 1e3,
        outcome.rows,
        outcome.values,
        outcome.checksum,
        peak_rss_kb() - rss_start_kb,
    );
}

/// `fill`: n single-row INSERTs — per-statement parameter cost.
fn case_fill<D: Driver>(layer: &str, backend: &str, backend_impl: &Backend, n: u64) {
    let mut driver = D::open(backend_impl, &format!("fill_{n}"));
    setup_table(&mut driver);
    let mut rng = Generator::new(0x5eed);
    let rss_start = peak_rss_kb();
    let start = Instant::now();
    let mut outcome = QueryOutcome {
        checksum: FNV_OFFSET,
        rows: 0,
        values: 0,
    };
    for id in 0..n as i64 {
        let (id, v, s) = rng.row3(id);
        let sql = "INSERT INTO bench_t (id, v, s) VALUES ($1, $2, $3)";
        driver
            .sql(sql, &[Value::Int(id), Value::Int(v), Value::String(s)])
            .expect("insert row");
        outcome.rows += 1;
    }
    report(
        &Ctx {
            layer,
            backend,
            case: "fill",
            n,
        },
        start.elapsed(),
        n,
        outcome,
        rss_start,
    );
    driver.close();
}

/// Rows per batched statement. Larger chunks trip the engine's 16 MiB
/// per-transaction write-set estimate, which is part of the current
/// contract rather than a benchmark artifact to work around.
const BULK_CHUNK: i64 = 5_000;

fn fill_chunks<D: Driver>(driver: &mut D, rng: &mut Generator, n: i64) -> u64 {
    let mut ops = 0u64;
    let mut values_text = String::with_capacity(BULK_CHUNK as usize * 40);
    for chunk_start in (0..n).step_by(BULK_CHUNK as usize) {
        values_text.clear();
        for id in chunk_start..(chunk_start + BULK_CHUNK).min(n) {
            let (id, v, s) = rng.row3(id);
            if !values_text.is_empty() {
                values_text.push(',');
            }
            values_text.push_str(&format!("({id},{v},'{}')", escape_sql(&s)));
        }
        driver
            .sql(
                &format!("INSERT INTO bench_t (id, v, s) VALUES {values_text}"),
                &[],
            )
            .expect("fill chunk");
        ops += 1;
    }
    ops
}

/// `fill_bulk`: the same rows through BULK_CHUNK-row statements —
/// batching benefit at the largest chunk the write-set budget admits.
fn case_fill_bulk<D: Driver>(layer: &str, backend: &str, backend_impl: &Backend, n: u64) {
    let mut driver = D::open(backend_impl, &format!("fill_bulk_{n}"));
    setup_table(&mut driver);
    let mut rng = Generator::new(0x5eed);
    let rss_start = peak_rss_kb();
    let start = Instant::now();
    let ops = fill_chunks(&mut driver, &mut rng, n as i64);
    let outcome = QueryOutcome {
        checksum: FNV_OFFSET,
        rows: n,
        values: 0,
    };
    report(
        &Ctx {
            layer,
            backend,
            case: "fill_bulk",
            n,
        },
        start.elapsed(),
        ops,
        outcome,
        rss_start,
    );
    driver.close();
}

/// `select`: scan n rows × 3 scalars — per-row/per-value result cost.
fn case_select<D: Driver>(layer: &str, backend: &str, backend_impl: &Backend, n: u64) {
    let mut driver = D::open(backend_impl, &format!("select_{n}"));
    setup_table(&mut driver);
    let mut rng = Generator::new(0x5eed);
    fill_chunks(&mut driver, &mut rng, n as i64);
    // Warm pass discarded, then the measured scan; a budget denial on
    // the warm pass is fine — the measured call reports it.
    let _ = driver.sql(SELECT_COLS, &[]);
    let rss_start = peak_rss_kb();
    let start = Instant::now();
    let outcome = driver.sql(SELECT_COLS, &[]);
    report_or_error(
        &Ctx {
            layer,
            backend,
            case: "select",
            n,
        },
        start.elapsed(),
        1,
        outcome,
        rss_start,
    );
    driver.close();
}

/// `point`: n single-row parameterized lookups — per-call fixed cost.
fn case_point<D: Driver>(layer: &str, backend: &str, backend_impl: &Backend, n: u64) {
    let mut driver = D::open(backend_impl, &format!("point_{n}"));
    setup_table(&mut driver);
    let mut rng = Generator::new(0x5eed);
    fill_chunks(&mut driver, &mut rng, n as i64);
    driver
        .sql(
            "SELECT id, v, s FROM bench_t WHERE id = $1",
            &[Value::Int(0)],
        )
        .expect("warm point");
    let rss_start = peak_rss_kb();
    let start = Instant::now();
    let mut outcome = QueryOutcome {
        checksum: FNV_OFFSET,
        rows: 0,
        values: 0,
    };
    for id in 0..n as i64 {
        let each = driver
            .sql(
                "SELECT id, v, s FROM bench_t WHERE id = $1",
                &[Value::Int(id)],
            )
            .expect("point query");
        outcome.checksum ^= each.checksum;
        outcome.rows += each.rows;
        outcome.values += each.values;
    }
    report(
        &Ctx {
            layer,
            backend,
            case: "point",
            n,
        },
        start.elapsed(),
        n,
        outcome,
        rss_start,
    );
    driver.close();
}

/// The 19 non-id `Wide` properties, shared by the CREATE projection and
/// the RETURN list so the two never drift.
const WIDE_PROPS: [&str; 19] = [
    "i0", "i1", "i2", "i3", "i4", "i5", "i6", "i7", "f0", "f1", "f2", "f3", "f4", "s0", "s1", "s2",
    "s3", "lst", "mp",
];

/// `wide`: n rows × 20 columns including strings, a list and a map —
/// nested value conversion. Fill is setup; the MATCH scan is measured.
fn case_wide<D: Driver>(layer: &str, backend: &str, backend_impl: &Backend, n: u64) {
    let mut driver = D::open(backend_impl, &format!("wide_{n}"));
    let mut rng = Generator::new(0x1eaf);
    let rows: Vec<Value> = (0..n as i64)
        .map(|id| Value::Map(rng.wide_props(id).into_iter().collect()))
        .collect();
    let mut create = String::from("UNWIND $rows AS row CREATE (w:Wide {id: row.id");
    for name in WIDE_PROPS {
        create.push_str(&format!(", {name}: row.{name}"));
    }
    create.push_str("})");
    for chunk in rows.chunks(BULK_CHUNK as usize) {
        driver
            .cypher(
                &create,
                &BTreeMap::from([("rows".to_string(), Value::List(chunk.to_vec()))]),
            )
            .expect("wide seed chunk");
    }
    let select = "MATCH (w:Wide) RETURN w.id, w.i0, w.i1, w.i2, w.i3, w.i4, w.i5, \
        w.i6, w.i7, w.f0, w.f1, w.f2, w.f3, w.f4, w.s0, w.s1, w.s2, w.s3, w.lst, w.mp";
    let _ = driver.cypher(select, &BTreeMap::new());
    let rss_start = peak_rss_kb();
    let start = Instant::now();
    let outcome = driver.cypher(select, &BTreeMap::new());
    report_or_error(
        &Ctx {
            layer,
            backend,
            case: "wide",
            n,
        },
        start.elapsed(),
        1,
        outcome,
        rss_start,
    );
    driver.close();
}

fn run_case<D: Driver>(layer: &str, backend: &Backend, case: &str, n: u64) {
    let backend_name = match backend {
        Backend::Memory => "memory",
        Backend::File(_) => "file",
    };
    match case {
        "fill" => case_fill::<D>(layer, backend_name, backend, n),
        "fill_bulk" => case_fill_bulk::<D>(layer, backend_name, backend, n),
        "select" => case_select::<D>(layer, backend_name, backend, n),
        "point" => case_point::<D>(layer, backend_name, backend, n),
        "wide" => case_wide::<D>(layer, backend_name, backend, n),
        other => eprintln!("unknown case: {other}"),
    }
}

fn main() {
    let sizes: Vec<u64> = env::var("HAWDB_BENCH_SIZES")
        .unwrap_or_else(|_| "1000".to_string())
        .split(',')
        .map(|s| s.parse().expect("HAWDB_BENCH_SIZES is integers"))
        .collect();
    let backends: Vec<String> = env::var("HAWDB_BENCH_BACKENDS")
        .unwrap_or_else(|_| "memory".to_string())
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let cases: Vec<String> = env::var("HAWDB_BENCH_CASES")
        .unwrap_or_else(|_| "fill,fill_bulk,select,point,wide".to_string())
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let layers: Vec<String> = env::var("HAWDB_BENCH_LAYERS")
        .unwrap_or_else(|_| "engine,ffi".to_string())
        .split(',')
        .map(|s| s.to_string())
        .collect();

    let temp_root = env::temp_dir().join(format!("hawdb_boundary_{}", std::process::id()));
    std::fs::create_dir_all(&temp_root).expect("create bench temp dir");

    for backend_name in &backends {
        let backend = match backend_name.as_str() {
            "memory" => Backend::Memory,
            "file" => {
                let dir = temp_root.join("file");
                std::fs::create_dir_all(&dir).expect("create file backend dir");
                Backend::File(dir)
            }
            other => {
                eprintln!("unknown backend: {other}");
                continue;
            }
        };
        for &n in &sizes {
            for case in &cases {
                for layer in &layers {
                    match layer.as_str() {
                        "engine" => run_case::<EngineDriver>(layer, &backend, case, n),
                        "ffi" => run_case::<FfiDriver>(layer, &backend, case, n),
                        "ffi_bin" => run_case::<FfiBinDriver>(layer, &backend, case, n),
                        other => eprintln!("unknown layer: {other}"),
                    }
                }
            }
        }
    }

    let _ = std::fs::remove_dir_all(&temp_root);
}
