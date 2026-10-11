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

// Package hawdb is a native Go binding for the HawDB embedded database.
//
// It calls libhawdb_ffi (the C ABI built from bindings/ffi) through purego,
// so it works with CGO_ENABLED=0 and needs no cgo toolchain. The shared
// library can be loaded explicitly with LoadLibrary; the package-level
// Open and Version lazily resolve a process-wide default from the
// HAWDB_LIBRARY environment variable, then the platform filename next to
// the working directory and the executable.
package hawdb

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// DB is an open HawDB database.
type DB struct {
	lib    *Library
	ptr    unsafe.Pointer
	mu     sync.RWMutex
	closed bool
}

// Error is the error type returned by this package. Kind carries the
// engine's error kind in snake_case (e.g. "capability_unavailable",
// "admission", "stopped"); "invalid_argument" marks a caller-side
// contract violation and "panic" a contained Rust panic.
type Error struct {
	Kind    string
	Message string
}

func (e *Error) Error() string {
	if e.Kind == "" {
		return e.Message
	}
	return e.Kind + ": " + e.Message
}

// OpenOptions controls how a database is opened.
type OpenOptions struct {
	// ReadOnly opens an existing database without permitting writes.
	ReadOnly bool
	// Memory opens an in-process database with no durable storage;
	// the path is ignored and no data survives Close.
	Memory bool
}

// Result is one materialized statement result: Columns in declaration
// order, and Rows as positional values aligned with Columns.
type Result struct {
	Columns []string
	Rows    [][]any
}

// Dicts returns the rows as maps keyed by column name.
func (r *Result) Dicts() []map[string]any {
	out := make([]map[string]any, len(r.Rows))
	for i, row := range r.Rows {
		m := make(map[string]any, len(r.Columns))
		for j, column := range r.Columns {
			if j < len(row) {
				m[column] = row[j]
			}
		}
		out[i] = m
	}
	return out
}

// Open opens a HawDB database at path on the default library, creating it
// if needed. Pass an OpenOptions to override defaults, e.g.
// Open(path, &OpenOptions{ReadOnly: true}).
func Open(path string, opts ...*OpenOptions) (*DB, error) {
	lib, err := defaultLibrary()
	if err != nil {
		return nil, err
	}
	return lib.Open(path, opts...)
}

// OpenReadOnly opens an existing database on the default library without
// permitting writes.
func OpenReadOnly(path string) (*DB, error) {
	return Open(path, &OpenOptions{ReadOnly: true})
}

// Open opens a HawDB database at path on this library.
func (lib *Library) Open(path string, opts ...*OpenOptions) (*DB, error) {
	var optionsJSON string
	if len(opts) > 0 && opts[0] != nil {
		data, err := json.Marshal(map[string]any{
			"read_only": opts[0].ReadOnly,
			"memory":    opts[0].Memory,
		})
		if err != nil {
			return nil, err
		}
		optionsJSON = string(data)
	}
	var errBuf ffiBuffer
	pathPtr, pathLen := strArg(path)
	optionsPtr, optionsLen := strArg(optionsJSON)
	ptr := lib.open(pathPtr, pathLen, optionsPtr, optionsLen, &errBuf)
	if ptr == nil {
		if err := lib.takeErr(&errBuf); err != nil {
			return nil, err
		}
		return nil, errors.New("hawdb: open failed")
	}
	db := &DB{lib: lib, ptr: ptr}
	// Safety net for callers that forget Close; Close is idempotent.
	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// OpenReadOnly opens an existing database on this library without
// permitting writes.
func (lib *Library) OpenReadOnly(path string) (*DB, error) {
	return lib.Open(path, &OpenOptions{ReadOnly: true})
}

// Query runs one Cypher statement. params maps $name parameters to values;
// supported types are nil, bool, integers, floats, string, []byte (stored
// as binary), []any and map[string]any.
func (db *DB) Query(cypher string, params map[string]any) (*Result, error) {
	return db.query(false, cypher, params)
}

// QuerySQL runs one SQL statement with positional parameters.
func (db *DB) QuerySQL(sql string, params ...any) (*Result, error) {
	return db.query(true, sql, params)
}

// Exec runs a statement that returns no interesting rows (CREATE, MERGE,
// SET, DDL), like database/sql's Exec.
func (db *DB) Exec(cypher string, params map[string]any) error {
	_, err := db.Query(cypher, params)
	return err
}

func (db *DB) query(sql bool, text string, params any) (*Result, error) {
	return db.queryWire(sql, text, params, false)
}

// queryBinary runs a statement and reads the compact binary result
// envelope instead of JSON. It requires a library built with the
// hawdb_query_ex export; otherwise it returns an error.
func (db *DB) queryBinary(sql bool, text string, params any) (*Result, error) {
	return db.queryWire(sql, text, params, true)
}

func (db *DB) queryWire(sql bool, text string, params any, binary bool) (*Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, &Error{Message: "hawdb: database is closed"}
	}
	var paramsJSON string
	if params != nil {
		data, err := json.Marshal(prepareValue(params))
		if err != nil {
			return nil, fmt.Errorf("hawdb: cannot marshal parameters: %w", err)
		}
		// A nil map or slice marshals to "null", not an object or array:
		// treat it as no parameters at all.
		if string(data) != "null" {
			paramsJSON = string(data)
		}
	}
	textPtr, textLen := strArg(text)
	paramsPtr, paramsLen := strArg(paramsJSON)
	var resultBuf, errBuf ffiBuffer
	var ok bool
	if binary {
		if db.lib.queryEx == nil {
			return nil, errors.New("hawdb: library lacks the extended query export")
		}
		dialect := ffiDialectCypher
		if sql {
			dialect = ffiDialectSQL
		}
		request := ffiQueryRequest{
			structSize: unsafe.Sizeof(ffiQueryRequest{}),
			dialect:    dialect,
			text:       textPtr,
			textLen:    textLen,
			params:     paramsPtr,
			paramsLen:  paramsLen,
			format:     ffiFormatBinary,
		}
		ok = db.lib.queryEx(db.ptr, &request, &resultBuf, &errBuf)
	} else if sql {
		ok = db.lib.querySQL(db.ptr, textPtr, textLen, paramsPtr, paramsLen, &resultBuf, &errBuf)
	} else {
		ok = db.lib.query(db.ptr, textPtr, textLen, paramsPtr, paramsLen, &resultBuf, &errBuf)
	}
	if !ok {
		if err := db.lib.takeErr(&errBuf); err != nil {
			return nil, err
		}
		return nil, errors.New("hawdb: query failed")
	}
	if binary {
		result, err := decodeResultBinary(db.lib.takeBytes(&resultBuf))
		if err != nil {
			return nil, fmt.Errorf("hawdb: malformed binary result: %w", err)
		}
		return result, nil
	}
	result, err := decodeResult(db.lib.takeBytes(&resultBuf))
	if err != nil {
		return nil, fmt.Errorf("hawdb: malformed result JSON: %w", err)
	}
	return result, nil
}

// UUID is a HawDB uuid value for parameters and results. It is the
// canonical hyphenated text form; the FFI boundary carries it as the
// tagged object {"$uuid": "<text>"}.
type UUID string

// prepareValue rewrites parameter values so types JSON cannot express
// reach the FFI boundary intact: []byte becomes {"$binary": "<base64>"}
// and UUID becomes {"$uuid": "<text>"}. It recurses into lists and maps.
func prepareValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return map[string]any{"$binary": base64.StdEncoding.EncodeToString(v)}
	case UUID:
		return map[string]any{"$uuid": string(v)}
	case float32:
		return floatParam(float64(v))
	case float64:
		return floatParam(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = prepareValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = prepareValue(item)
		}
		return out
	default:
		return value
	}
}

// floatParam renders a float so its JSON text always carries a decimal
// point or exponent. encoding/json would marshal float64(14000) as
// "14000", which the engine decodes as an integer, silently changing the
// parameter's type. Non-finite values stay untouched so Marshal keeps
// surfacing its usual error.
func floatParam(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	text := strconv.FormatFloat(v, 'g', -1, 64)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return json.Number(text)
}

// decodeResult streams the {"columns": [...], "rows": [[...], ...]}
// envelope into a Result in one pass. Values are normalized while they
// decode — numbers land as int64 or float64, the {"$binary"} and
// {"$uuid"} tagged objects as []byte and UUID — so there is no
// intermediate DOM and no second traversal over every value.
func decodeResult(data []byte) (*Result, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("expected a JSON object")
	}
	result := &Result{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch key {
		case "columns":
			if err := dec.Decode(&result.Columns); err != nil {
				return nil, err
			}
		case "rows":
			if err := decodeRows(dec, result); err != nil {
				return nil, err
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, err
			}
		}
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return nil, err
	}
	return result, nil
}

func decodeRows(dec *json.Decoder, result *Result) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return errors.New("expected a rows array")
	}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			return errors.New("expected a row array")
		}
		var row []any
		for dec.More() {
			value, err := decodeValue(dec)
			if err != nil {
				return err
			}
			row = append(row, value)
		}
		if _, err := dec.Token(); err != nil { // ']'
			return err
		}
		result.Rows = append(result.Rows, row)
	}
	_, err = dec.Token() // ']'
	return err
}

func decodeValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := token.(type) {
	case json.Delim:
		if t == '[' {
			var list []any
			for dec.More() {
				item, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				list = append(list, item)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return list, nil
		}
		object := make(map[string]any)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			value, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			object[key.(string)] = value
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		// Tagged single-key objects decode back to their types; any
		// other shape stays a plain map.
		if len(object) == 1 {
			if encoded, ok := object["$binary"].(string); ok {
				if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
					return decoded, nil
				}
			}
			if encoded, ok := object["$uuid"].(string); ok {
				return UUID(encoded), nil
			}
		}
		return object, nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, _ := t.Float64()
		return f, nil
	default:
		return t, nil
	}
}

// ------------------------------------------------------------------
// Binary result envelope — mirrors the encoder in the FFI crate.
// "HDB1" | u32 ncols | per column u32 len + name | u64 nrows | per
// row, per column one tagged value:
//   0 null · 1 bool u8 · 2 i64 · 3 f64 · 4 string (u64 len + bytes)
//   5 binary (u64 len + bytes) · 6 uuid (16 bytes) · 7 list (u64
//   count + items) · 8 map (u64 count; u32 key len + key + value)
// ------------------------------------------------------------------

type binReader struct {
	buf []byte
	pos int
}

func (r *binReader) take(n int) ([]byte, error) {
	// n > remaining can only hold when n came from a hostile length
	// field; written this way pos+n cannot overflow.
	if n < 0 || n > len(r.buf)-r.pos {
		return nil, errors.New("truncated envelope")
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *binReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *binReader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

// decodeResultBinary reads the tagged binary envelope straight into a
// Result — no JSON parsing, no intermediate DOM.
func decodeResultBinary(data []byte) (*Result, error) {
	r := &binReader{buf: data}
	magic, err := r.take(4)
	if err != nil || string(magic) != "HDB1" {
		return nil, errors.New("bad magic")
	}
	ncols, err := r.u32()
	if err != nil {
		return nil, err
	}
	result := &Result{}
	for i := uint32(0); i < ncols; i++ {
		nameLen, err := r.u32()
		if err != nil {
			return nil, err
		}
		name, err := r.take(int(nameLen))
		if err != nil {
			return nil, err
		}
		result.Columns = append(result.Columns, string(name))
	}
	nrows, err := r.u64()
	if err != nil {
		return nil, err
	}
	// Every row costs at least one tag byte per column, so a declared
	// nrows beyond the remaining bytes can never be satisfied.
	if nrows > uint64(len(r.buf)-r.pos) {
		return nil, errors.New("truncated envelope")
	}
	for i := uint64(0); i < nrows; i++ {
		row := make([]any, 0, ncols)
		for j := uint32(0); j < ncols; j++ {
			value, err := decodeBinValue(r, 0)
			if err != nil {
				return nil, err
			}
			row = append(row, value)
		}
		result.Rows = append(result.Rows, row)
	}
	if r.pos != len(r.buf) {
		return nil, errors.New("trailing bytes after envelope")
	}
	return result, nil
}

func decodeBinValue(r *binReader, depth int) (any, error) {
	// Tags cost a byte each, so depth is already bounded by the input
	// length; the cap just keeps a hostile payload off the stack.
	if depth > 64 {
		return nil, errors.New("envelope nesting too deep")
	}
	tag, err := r.take(1)
	if err != nil {
		return nil, err
	}
	switch tag[0] {
	case 0:
		return nil, nil
	case 1:
		b, err := r.take(1)
		if err != nil {
			return nil, err
		}
		return b[0] != 0, nil
	case 2:
		u, err := r.u64()
		return int64(u), err
	case 3:
		u, err := r.u64()
		return math.Float64frombits(u), err
	case 4:
		n, err := r.u64()
		if err != nil {
			return nil, err
		}
		b, err := r.take(int(n))
		return string(b), err
	case 5:
		n, err := r.u64()
		if err != nil {
			return nil, err
		}
		b, err := r.take(int(n))
		return bytes.Clone(b), err
	case 6:
		b, err := r.take(16)
		if err != nil {
			return nil, err
		}
		var uuid [16]byte
		copy(uuid[:], b)
		return UUID(fmt.Sprintf("%x-%x-%x-%x-%x",
			uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])), nil
	case 7:
		n, err := r.u64()
		if err != nil {
			return nil, err
		}
		if n > uint64(len(r.buf)-r.pos) {
			return nil, errors.New("truncated envelope")
		}
		list := make([]any, 0, n)
		for i := uint64(0); i < n; i++ {
			item, err := decodeBinValue(r, depth+1)
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		return list, nil
	case 8:
		n, err := r.u64()
		if err != nil {
			return nil, err
		}
		// Each entry needs at least a u32 key length and a value tag.
		if n > uint64(len(r.buf)-r.pos)/5 {
			return nil, errors.New("truncated envelope")
		}
		object := make(map[string]any, n)
		for i := uint64(0); i < n; i++ {
			keyLen, err := r.u32()
			if err != nil {
				return nil, err
			}
			key, err := r.take(int(keyLen))
			if err != nil {
				return nil, err
			}
			value, err := decodeBinValue(r, depth+1)
			if err != nil {
				return nil, err
			}
			object[string(key)] = value
		}
		return object, nil
	default:
		return nil, fmt.Errorf("unknown value tag %d", tag[0])
	}
}

// Close releases the database handle. It is safe to call more than once.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	db.lib.close(db.ptr)
	db.ptr = nil
	return nil
}

// Version returns the HawDB version string reported by this library.
func (lib *Library) Version() string {
	var out ffiBuffer
	lib.version(&out)
	return string(lib.takeBytes(&out))
}

// Version returns the HawDB version string of the default library, or an
// empty string if it cannot be loaded.
func Version() string {
	lib, err := defaultLibrary()
	if err != nil {
		return ""
	}
	return lib.Version()
}
