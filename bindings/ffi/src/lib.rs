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

//! C ABI bindings for the HawDB embedded database.
//!
//! The surface is deliberately small: open a database, run one Cypher or SQL
//! statement per call, close. All strings cross the boundary as UTF-8
//! pointer-plus-length pairs; there is no NUL-termination contract for
//! inputs, though every returned buffer carries a NUL byte just past
//! its `len` as a courtesy for C callers.
//! Statement parameters and results are exchanged as JSON, so hosts never
//! need to know HawDB's internal value representation.
//!
//! Output convention: results and errors are delivered through caller-owned
//! [`HawdbBuffer`] out parameters, never a thread-local — callers such as Go
//! may hop OS threads between calls. Every buffer the library writes is
//! heap-allocated and must be released exactly once with
//! [`hawdb_buffer_free`]. On failure a query fills `err_out` with a JSON
//! object `{"kind": "<engine error kind>", "message": "<text>"}` carrying
//! the same error kind the engine reports; a failed statement leaves the
//! database handle usable. Passing NULL for an out parameter discards that
//! output.

use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::sync::Mutex;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hawdb::{
    EmbeddedQueryError, HawDBEmbedded, HawDBEmbeddedOpenOptions, HawDBError, QueryOutput, Uuid,
    Value,
};
use serde_json::json;

mod envelope;

/// An open HawDB database handle. Opaque to C callers.
pub struct HawdbDatabase {
    inner: Mutex<HawDBEmbedded>,
}

/// A byte buffer the library hands to the caller.
///
/// `len` counts the content bytes only; the library also writes a NUL
/// byte at `ptr[len]` as a courtesy for C callers, and binary content
/// may itself contain interior NULs. Every non-NULL `ptr` is owned by
/// the caller and released exactly once with [`hawdb_buffer_free`] —
/// there is one free function for every buffer kind. The library
/// never retains or reuses a buffer it has returned.
#[repr(C)]
pub struct HawdbBuffer {
    pub ptr: *mut c_char,
    pub len: usize,
}

/// An error reported by this crate: an engine or contract `kind` plus a
/// human-readable `message`.
struct FfiError {
    kind: String,
    message: String,
}

impl FfiError {
    /// A caller-side contract violation: NULL pointer, non-UTF-8 bytes,
    /// malformed JSON, wrong parameter container.
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: "invalid_argument".to_string(),
            message: message.into(),
        }
    }

    fn panic() -> Self {
        Self {
            kind: "panic".to_string(),
            message: "hawdb: panic while executing statement".to_string(),
        }
    }

    /// A query error carrying the engine's own kind: the `HawDBError`
    /// variant for database failures, `admission` or `stopped` otherwise.
    fn query(error: &EmbeddedQueryError) -> Self {
        let kind = match error {
            EmbeddedQueryError::Database(inner) => variant_name(inner),
            EmbeddedQueryError::Admission(_) => "admission".to_string(),
            EmbeddedQueryError::Stopped(_) => "stopped".to_string(),
        };
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn open(error: &HawDBError) -> Self {
        Self {
            kind: variant_name(error),
            message: error.to_string(),
        }
    }
}

/// Extract the enum variant name of an error as snake_case, e.g.
/// `CapabilityUnavailable` -> `capability_unavailable`. Derived from the
/// `Debug` representation so it stays in sync with the engine's taxonomy.
fn variant_name(error: &impl std::fmt::Debug) -> String {
    let text = format!("{error:?}");
    let end = text.find(['(', ' ', '{']).unwrap_or(text.len());
    let mut snake = String::with_capacity(end + 4);
    for (index, ch) in text[..end].chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                snake.push('_');
            }
            snake.push(ch.to_ascii_lowercase());
        } else {
            snake.push(ch);
        }
    }
    snake
}

/// Write `text` into `*out` as a heap-allocated buffer. A NULL `out`
/// discards the output. Interior NUL bytes are replaced so the buffer is
/// always NUL-terminated; `len` stays authoritative.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_buffer(out: *mut HawdbBuffer, text: impl std::fmt::Display) {
    let mut text = text.to_string();
    if text.contains('\0') {
        text = text.replace('\0', " ");
    }
    unsafe { set_bytes(out, text.into_bytes()) };
}

/// Hand `bytes` to `*out`. Every buffer the library returns owns
/// `len + 1` bytes — `len` bytes of content plus a trailing NUL
/// sentinel at `ptr[len]` that is never counted in `len` but lets C
/// callers treat text results as C strings. `len` is authoritative and
/// binary content may contain interior NULs. Released with
/// [`hawdb_buffer_free`]; the library never retains what it returns.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_bytes(out: *mut HawdbBuffer, mut bytes: Vec<u8>) {
    if out.is_null() {
        return;
    }
    bytes.push(0);
    let mut boxed = bytes.into_boxed_slice();
    let buffer = unsafe { &mut *out };
    buffer.len = boxed.len() - 1;
    buffer.ptr = boxed.as_mut_ptr() as *mut c_char;
    std::mem::forget(boxed);
}

/// Zero an out buffer so callers that read it unconditionally see {NULL, 0}.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn clear_buffer(out: *mut HawdbBuffer) {
    if !out.is_null() {
        unsafe {
            (*out).ptr = ptr::null_mut();
            (*out).len = 0;
        }
    }
}

/// Deliver `error` through `err_out` as `{"kind", "message"}` JSON.
/// A NULL `err_out` discards it.
///
/// # Safety
/// `err_out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_error(err_out: *mut HawdbBuffer, error: FfiError) {
    let payload = json!({ "kind": error.kind, "message": error.message });
    unsafe { set_buffer(err_out, payload) };
}

/// A borrowed `(ptr, len)` string argument at the FFI boundary.
/// A NULL `ptr` is only meaningful where the callee treats it as absent.
#[derive(Clone, Copy)]
struct FfiStr {
    ptr: *const c_char,
    len: usize,
}

/// Read a required UTF-8 string argument.
///
/// # Safety
/// `text.ptr` must be readable for `text.len` bytes, or NULL.
unsafe fn read_str<'a>(text: FfiStr, name: &str) -> Result<&'a str, FfiError> {
    if text.ptr.is_null() {
        return Err(FfiError::invalid(format!("hawdb: {name} must not be NULL")));
    }
    let bytes = unsafe { std::slice::from_raw_parts(text.ptr.cast::<u8>(), text.len) };
    std::str::from_utf8(bytes)
        .map_err(|_| FfiError::invalid(format!("hawdb: {name} is not valid UTF-8")))
}

/// Read an optional UTF-8 argument: a NULL pointer means absent.
///
/// # Safety
/// `text.ptr` must be readable for `text.len` bytes, or NULL.
unsafe fn read_opt_str<'a>(text: FfiStr, name: &str) -> Result<Option<&'a str>, FfiError> {
    if text.ptr.is_null() {
        return Ok(None);
    }
    unsafe { read_str(text, name) }.map(Some)
}

/// Serialize a HawDB [`Value`] with the boundary's tagged encoding —
/// `Binary` becomes `{"$binary": "<base64>"}` and `Uuid` becomes
/// `{"$uuid": "<canonical>"}` so both stay distinguishable from plain
/// strings. Serializing straight from the value avoids materializing a
/// `serde_json::Value` DOM per row.
struct ValueJson<'a>(&'a Value);

impl Serialize for ValueJson<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(item) => serializer.serialize_bool(*item),
            Value::Int(item) => serializer.serialize_i64(*item),
            Value::Float(item) if item.is_finite() => serializer.serialize_f64(*item),
            Value::Float(_) => serializer.serialize_unit(),
            Value::String(item) => serializer.serialize_str(item),
            Value::Binary(item) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("$binary", &BASE64.encode(item))?;
                map.end()
            }
            Value::Uuid(item) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("$uuid", &item.to_string())?;
                map.end()
            }
            Value::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&ValueJson(item))?;
                }
                seq.end()
            }
            Value::Map(items) => {
                let mut map = serializer.serialize_map(Some(items.len()))?;
                for (key, item) in items {
                    map.serialize_entry(key, &ValueJson(item))?;
                }
                map.end()
            }
        }
    }
}

struct RowJson<'a>(&'a [Value]);

impl Serialize for RowJson<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            seq.serialize_element(&ValueJson(item))?;
        }
        seq.end()
    }
}

/// Serialize a [`QueryOutput`] as the result envelope
/// `{"columns": [...], "rows": [[...], ...]}` without building an
/// intermediate DOM.
struct OutputJson<'a>(&'a QueryOutput);

impl Serialize for OutputJson<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        struct Rows<'a>(&'a QueryOutput);
        impl Serialize for Rows<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut seq = s.serialize_seq(None)?;
                for row in self.0.value_rows() {
                    seq.serialize_element(&RowJson(row))?;
                }
                seq.end()
            }
        }
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("columns", &self.0.schema().columns())?;
        map.serialize_entry("rows", &Rows(self.0))?;
        map.end()
    }
}

/// Convert a `serde_json::Value` into a HawDB [`Value`] for parameters.
///
/// This is the inverse of [`value_to_json`]: the single-key objects
/// `{"$binary": "<base64>"}` and `{"$uuid": "<canonical>"}` decode to
/// `Binary` and `Uuid`; every other object becomes a `Map`.
fn json_to_value(value: &serde_json::Value) -> Result<Value, String> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(item) => Value::Bool(*item),
        serde_json::Value::Number(item) => {
            if let Some(item) = item.as_i64() {
                Value::Int(item)
            } else if let Some(item) = item.as_f64() {
                Value::Float(item)
            } else {
                return Err("number is out of range".to_string());
            }
        }
        serde_json::Value::String(item) => Value::String(item.clone()),
        serde_json::Value::Array(items) => {
            Value::List(items.iter().map(json_to_value).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(items) => {
            if items.len() == 1 {
                if let Some(encoded) = items.get("$binary").and_then(|v| v.as_str()) {
                    return BASE64
                        .decode(encoded)
                        .map(Value::Binary)
                        .map_err(|error| format!("invalid $binary parameter: {error}"));
                }
                if let Some(encoded) = items.get("$uuid").and_then(|v| v.as_str()) {
                    return encoded
                        .parse::<Uuid>()
                        .map(Value::Uuid)
                        .map_err(|error| format!("invalid $uuid parameter: {error}"));
                }
            }
            Value::Map(
                items
                    .iter()
                    .map(|(key, item)| Ok((key.clone(), json_to_value(item)?)))
                    .collect::<Result<BTreeMap<_, _>, String>>()?,
            )
        }
    })
}

/// Serialize `output` into `*out`'s buffer as the result envelope
/// `{"columns": [...], "rows": [[...], ...]}`, streaming values instead
/// of building a DOM.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_json_buffer(out: *mut HawdbBuffer, output: &QueryOutput) {
    let mut bytes = Vec::new();
    if serde_json::to_writer(&mut bytes, &OutputJson(output)).is_err() {
        unsafe { clear_buffer(out) };
        return;
    }
    unsafe { set_bytes(out, bytes) };
}

/// Open a HawDB database at `path`, creating it if needed.
///
/// `path` is `path_len` UTF-8 bytes and must not be NULL. `options_json`
/// may be NULL or a JSON object; currently supported keys:
/// `{"read_only": bool, "memory": bool}`. With `"memory": true` the
/// database lives in process memory only; `path` must still be a valid
/// string but is not used for storage.
///
/// Returns a handle to pass to the other functions, or NULL on failure
/// with `err_out` filled. Close the handle with [`hawdb_close`].
///
/// # Safety
/// `path` and `options_json` must be readable for `path_len` /
/// `options_len` bytes or NULL; `err_out` must be NULL or writable
/// [`HawdbBuffer`] storage. The returned handle is safe to use from
/// multiple threads; calls are serialized internally.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_open(
    path: *const c_char,
    path_len: usize,
    options_json: *const c_char,
    options_len: usize,
    err_out: *mut HawdbBuffer,
) -> *mut HawdbDatabase {
    unsafe { clear_buffer(err_out) };
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        hawdb_open_inner(
            FfiStr {
                ptr: path,
                len: path_len,
            },
            FfiStr {
                ptr: options_json,
                len: options_len,
            },
        )
    }));
    match result {
        Ok(Ok(db)) => db,
        Ok(Err(error)) => {
            unsafe { set_error(err_out, error) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { set_error(err_out, FfiError::panic()) };
            ptr::null_mut()
        }
    }
}

unsafe fn hawdb_open_inner(
    path: FfiStr,
    options_json: FfiStr,
) -> Result<*mut HawdbDatabase, FfiError> {
    let path = unsafe { read_str(path, "path") }?;
    let mut options = HawDBEmbeddedOpenOptions::new(PathBuf::from(path));
    if let Some(options_text) = unsafe { read_opt_str(options_json, "options_json") }? {
        let parsed: serde_json::Value = serde_json::from_str(options_text).map_err(|error| {
            FfiError::invalid(format!("hawdb: options_json is not valid JSON: {error}"))
        })?;
        let object = parsed
            .as_object()
            .ok_or_else(|| FfiError::invalid("hawdb: options_json must be a JSON object"))?;
        if let Some(read_only) = object.get("read_only") {
            let read_only = read_only
                .as_bool()
                .ok_or_else(|| FfiError::invalid("hawdb: \"read_only\" must be a bool"))?;
            options.config.read_only = read_only;
        }
        if let Some(memory) = object.get("memory") {
            let memory = memory
                .as_bool()
                .ok_or_else(|| FfiError::invalid("hawdb: \"memory\" must be a bool"))?;
            if memory {
                let database = HawDBEmbedded::open_in_memory_with_config(options.config);
                return Ok(Box::into_raw(Box::new(HawdbDatabase {
                    inner: Mutex::new(database),
                })));
            }
        }
    }
    let database =
        HawDBEmbedded::open_with_options(options).map_err(|error| FfiError::open(&error))?;
    Ok(Box::into_raw(Box::new(HawdbDatabase {
        inner: Mutex::new(database),
    })))
}

/// Close a database handle and release its resources.
///
/// # Safety
/// `db` must be a handle returned by [`hawdb_open`] and not already
/// closed. Passing NULL is a no-op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_close(db: *mut HawdbDatabase) {
    if db.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(db) })));
}

/// Run one Cypher statement.
///
/// `cypher` is `cypher_len` UTF-8 bytes and must not be NULL.
/// `params_json` may be NULL or a JSON object mapping `$name` parameter
/// names to values (`null`, bool, number, string, array, object, or the
/// tagged forms `{"$binary": "<base64>"}` and `{"$uuid": "<text>"}`).
///
/// On success returns true and `result_out` receives the result envelope
/// `{"columns": [...], "rows": [[...], ...]}`; on failure returns false
/// and `err_out` receives `{"kind", "message"}`.
///
/// # Safety
/// `db` must be a live [`hawdb_open`] handle; `cypher` and `params_json`
/// must be readable for `cypher_len` / `params_len` bytes or NULL;
/// `result_out` and `err_out` must be NULL or writable [`HawdbBuffer`]
/// storage. A false return leaves the handle usable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_query(
    db: *mut HawdbDatabase,
    cypher: *const c_char,
    cypher_len: usize,
    params_json: *const c_char,
    params_len: usize,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        query_impl(
            db,
            FfiStr {
                ptr: cypher,
                len: cypher_len,
            },
            FfiStr {
                ptr: params_json,
                len: params_len,
            },
            Dialect::Cypher,
            ResultFormat::Json,
            result_out,
            err_out,
        )
    }
}

/// Run one SQL statement.
///
/// `params_json` may be NULL or a JSON array of positional `$1`
/// parameters with the same value encoding as [`hawdb_query`]. Result
/// and error conventions match [`hawdb_query`].
///
/// # Safety
/// Same contract as [`hawdb_query`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_query_sql(
    db: *mut HawdbDatabase,
    sql: *const c_char,
    sql_len: usize,
    params_json: *const c_char,
    params_len: usize,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        query_impl(
            db,
            FfiStr {
                ptr: sql,
                len: sql_len,
            },
            FfiStr {
                ptr: params_json,
                len: params_len,
            },
            Dialect::Sql,
            ResultFormat::Json,
            result_out,
            err_out,
        )
    }
}

/// A full statement request for [`hawdb_query_ex`]: the text, its
/// dialect and parameters, and the desired result envelope — the
/// Vulkan-style pattern where the call signature stays fixed and the
/// request descriptor grows.
///
/// Callers set `struct_size` to `sizeof(HawdbQueryRequest)`; the
/// library reads only the fields that fit inside it, so newer releases
/// can append fields without breaking older callers. `dialect` and
/// `format` default to Cypher and JSON when they fall outside
/// `struct_size`.
#[repr(C)]
pub struct HawdbQueryRequest {
    /// `sizeof(HawdbQueryRequest)` as seen by the caller.
    pub struct_size: usize,
    /// `HAWDB_DIALECT_*` value; Cypher when the field is absent.
    pub dialect: u32,
    /// Statement text: `text_len` UTF-8 bytes at `text`.
    pub text: *const c_char,
    pub text_len: usize,
    /// JSON parameters: object for Cypher, positional array for SQL;
    /// NULL or empty for none.
    pub params_json: *const c_char,
    pub params_len: usize,
    /// `HAWDB_FORMAT_*` value; JSON when the field is absent.
    pub format: u32,
}

/// Cypher statement dialect for [`HawdbQueryRequest`].
pub const HAWDB_DIALECT_CYPHER: u32 = 0;
/// SQL statement dialect for [`HawdbQueryRequest`].
pub const HAWDB_DIALECT_SQL: u32 = 1;
/// JSON result envelope for [`HawdbQueryRequest`].
pub const HAWDB_FORMAT_JSON: u32 = 0;
/// Binary result envelope for [`HawdbQueryRequest`] — see `src/envelope.rs`.
pub const HAWDB_FORMAT_BINARY: u32 = 1;

/// Read `request` into its parts, honoring `struct_size` so a caller
/// compiled against a shorter request keeps defaults for the fields it
/// does not carry.
///
/// # Safety
/// `request` must be readable for `request->struct_size` bytes.
unsafe fn read_query_request(
    request: *const HawdbQueryRequest,
) -> Result<(FfiStr, FfiStr, Dialect, ResultFormat), FfiError> {
    let size = unsafe { (*request).struct_size };
    let need = |offset: usize| size >= offset;
    if !need(std::mem::offset_of!(HawdbQueryRequest, text) + 2 * std::mem::size_of::<usize>()) {
        return Err(FfiError::invalid(format!(
            "HawdbQueryRequest size {size} is too small for the statement fields"
        )));
    }
    let dialect = if need(std::mem::offset_of!(HawdbQueryRequest, dialect) + 4) {
        match unsafe { (*request).dialect } {
            HAWDB_DIALECT_CYPHER => Dialect::Cypher,
            HAWDB_DIALECT_SQL => Dialect::Sql,
            other => return Err(FfiError::invalid(format!("unknown dialect {other}"))),
        }
    } else {
        Dialect::Cypher
    };
    let format = if need(std::mem::offset_of!(HawdbQueryRequest, format) + 4) {
        match unsafe { (*request).format } {
            HAWDB_FORMAT_JSON => ResultFormat::Json,
            HAWDB_FORMAT_BINARY => ResultFormat::Binary,
            other => return Err(FfiError::invalid(format!("unknown result format {other}"))),
        }
    } else {
        ResultFormat::Json
    };
    let params = if need(
        std::mem::offset_of!(HawdbQueryRequest, params_json) + 2 * std::mem::size_of::<usize>(),
    ) {
        unsafe {
            FfiStr {
                ptr: (*request).params_json,
                len: (*request).params_len,
            }
        }
    } else {
        FfiStr {
            ptr: ptr::null(),
            len: 0,
        }
    };
    Ok((
        unsafe {
            FfiStr {
                ptr: (*request).text,
                len: (*request).text_len,
            }
        },
        params,
        dialect,
        format,
    ))
}

/// Which query language the statement text is written in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    Cypher,
    Sql,
}

/// Result payload encoding for the query entry points.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ResultFormat {
    /// `{"columns": [...], "rows": [[...], ...]}` JSON text.
    Json,
    /// The compact tagged binary envelope written by
    /// [`envelope::encode_output_binary`].
    Binary,
}

/// Run one statement described by `request`.
///
/// The request carries everything the call needs — dialect, text,
/// parameters, result envelope — so this signature never grows. The
/// simple entry points [`hawdb_query`] and [`hawdb_query_sql`] remain
/// for calls that only need the defaults. The binary envelope's exact
/// layout is documented in `src/envelope.rs`.
///
/// # Safety
/// `db` must be a live [`hawdb_open`] handle; `request` must not be
/// NULL and must be readable for `request->struct_size` bytes, and its
/// `text`/`params_json` for the recorded lengths; `result_out` and
/// `err_out` must be NULL or writable [`HawdbBuffer`] storage. A false
/// return leaves the handle usable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_query_ex(
    db: *mut HawdbDatabase,
    request: *const HawdbQueryRequest,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    if request.is_null() {
        unsafe { set_error(err_out, FfiError::invalid("request must not be NULL")) };
        return false;
    }
    let (text, params_json, dialect, format) = match unsafe { read_query_request(request) } {
        Ok(parts) => parts,
        Err(error) => {
            unsafe { set_error(err_out, error) };
            return false;
        }
    };
    unsafe { query_impl(db, text, params_json, dialect, format, result_out, err_out) }
}

unsafe fn query_impl(
    db: *mut HawdbDatabase,
    text: FfiStr,
    params_json: FfiStr,
    dialect: Dialect,
    format: ResultFormat,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        clear_buffer(result_out);
        clear_buffer(err_out);
    }
    if db.is_null() {
        unsafe { set_error(err_out, FfiError::invalid("hawdb: database handle is NULL")) };
        return false;
    }
    let database = unsafe { &*db };
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        let text = read_str(text, "statement")?;
        if matches!(dialect, Dialect::Sql) {
            let params = parse_sql_params(params_json)?;
            database
                .inner
                .lock()
                .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))?
                .database_mut()
                .query_sql_with_params(text, &params)
                .map_err(|error| FfiError {
                    kind: variant_name(&error),
                    message: error.to_string(),
                })
        } else {
            let params = parse_cypher_params(params_json)?;
            database
                .inner
                .lock()
                .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))?
                .query_with_params_admitted(text, &params)
                .map_err(|error| FfiError::query(&error))
        }
    }));
    match result {
        Ok(Ok(output)) => {
            match format {
                ResultFormat::Json => unsafe { set_json_buffer(result_out, &output) },
                ResultFormat::Binary => unsafe {
                    set_bytes(result_out, envelope::encode_output_binary(&output))
                },
            }
            true
        }
        Ok(Err(error)) => {
            unsafe { set_error(err_out, error) };
            false
        }
        Err(_) => {
            unsafe { set_error(err_out, FfiError::panic()) };
            false
        }
    }
}

unsafe fn parse_cypher_params(params_json: FfiStr) -> Result<BTreeMap<String, Value>, FfiError> {
    let Some(text) = unsafe { read_opt_str(params_json, "params_json") }? else {
        return Ok(BTreeMap::new());
    };
    parsed_params(text, "cypher params must be a JSON object")?
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, item)| {
            json_to_value(item)
                .map(|value| (key.clone(), value))
                .map_err(FfiError::invalid)
        })
        .collect()
}

unsafe fn parse_sql_params(params_json: FfiStr) -> Result<Vec<Value>, FfiError> {
    let Some(text) = unsafe { read_opt_str(params_json, "params_json") }? else {
        return Ok(Vec::new());
    };
    parsed_params(text, "sql params must be a JSON array")?
        .as_array()
        .unwrap()
        .iter()
        .map(|item| json_to_value(item).map_err(FfiError::invalid))
        .collect()
}

fn parsed_params(text: &str, requirement: &str) -> Result<serde_json::Value, FfiError> {
    let parsed: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        FfiError::invalid(format!("hawdb: params_json is not valid JSON: {error}"))
    })?;
    let matches = if requirement.contains("object") {
        parsed.is_object()
    } else {
        parsed.is_array()
    };
    if !matches {
        return Err(FfiError::invalid(format!("hawdb: {requirement}")));
    }
    Ok(parsed)
}

/// Free a buffer returned by this library and zero the caller's
/// [`HawdbBuffer`] so a second call is a safe no-op. The library owns
/// `len + 1` bytes behind `ptr` (content plus the NUL sentinel), so
/// this must be the function that releases it. Passing NULL for
/// `buffer`, or a buffer whose `ptr` is NULL, is a no-op.
///
/// # Safety
/// `buffer` must be NULL or point to a [`HawdbBuffer`] whose `ptr` was
/// allocated by this library and not already freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_buffer_free(buffer: *mut HawdbBuffer) {
    if buffer.is_null() {
        return;
    }
    let buffer = unsafe { &mut *buffer };
    if buffer.ptr.is_null() {
        return;
    }
    drop(unsafe {
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            buffer.ptr as *mut u8,
            buffer.len + 1,
        ))
    });
    buffer.ptr = ptr::null_mut();
    buffer.len = 0;
}

/// Copy the HawDB version string into `out`. The buffer is caller-owned
/// like every other [`HawdbBuffer`]: release it with
/// [`hawdb_buffer_free`].
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_version(out: *mut HawdbBuffer) {
    unsafe { set_buffer(out, concat!("hawdb ", env!("CARGO_PKG_VERSION"))) };
}
