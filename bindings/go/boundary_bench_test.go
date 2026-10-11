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

package hawdb

// Host-boundary benchmark for issue #976: the Go side of the
// measurements the C ABI's benches/boundary.rs records for the engine
// and FFI layers. Same five cases, same deterministic dataset, same
// canonical value checksum — so `go` rows can be compared line by line
// against the `engine`/`ffi` lines for value parity.
//
// Disabled by default; run with:
//
//	HAWDB_BENCH=1 HAWDB_LIBRARY=/path/to/libhawdb_ffi.dylib \
//	  go test -run TestBoundaryBench -v
//
// Knobs mirror the Rust bench: HAWDB_BENCH_SIZES (comma list, default
// 1000), HAWDB_BENCH_BACKENDS (comma list of memory,file, default
// memory), HAWDB_BENCH_CASES (comma list, default all five),
// HAWDB_BENCH_WIRE (comma list of json,bin; json is the default and
// reports layer=go, bin runs the experimental binary envelope as
// layer=go_bin and needs a library with the hawdb_query_*_bin exports).

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	fnvOffset = 0xcbf29ce484222325
	fnvPrime  = 0x100000001b3
)

func benchHashBytes(h *uint64, b []byte) {
	for _, c := range b {
		*h ^= uint64(c)
		*h *= fnvPrime
	}
}

func benchHashLen(h *uint64, n int) {
	var le [8]byte
	u := uint64(n)
	for i := range le {
		le[i] = byte(u)
		u >>= 8
	}
	benchHashBytes(h, le[:])
}

// Canonical value hash mirroring the Rust bench's hash_value: type tag
// plus little-endian/UTF-8 payload; map keys sorted to match BTreeMap.
func benchHashValue(h *uint64, v any) {
	switch t := v.(type) {
	case nil:
		benchHashBytes(h, []byte{0})
	case bool:
		b := byte(0)
		if t {
			b = 1
		}
		benchHashBytes(h, []byte{1, b})
	case int64:
		var le [8]byte
		u := uint64(t)
		for i := range le {
			le[i] = byte(u)
			u >>= 8
		}
		benchHashBytes(h, []byte{2})
		benchHashBytes(h, le[:])
	case float64:
		var le [8]byte
		u := math.Float64bits(t)
		for i := range le {
			le[i] = byte(u)
			u >>= 8
		}
		benchHashBytes(h, []byte{3})
		benchHashBytes(h, le[:])
	case string:
		benchHashBytes(h, []byte{4})
		benchHashLen(h, len(t))
		benchHashBytes(h, []byte(t))
	case []byte:
		benchHashBytes(h, []byte{5})
		benchHashLen(h, len(t))
		benchHashBytes(h, t)
	case UUID:
		benchHashBytes(h, []byte{6})
		benchHashBytes(h, []byte(t))
	case []any:
		benchHashBytes(h, []byte{7})
		benchHashLen(h, len(t))
		for _, item := range t {
			benchHashValue(h, item)
		}
	case map[string]any:
		benchHashBytes(h, []byte{8})
		benchHashLen(h, len(t))
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			benchHashLen(h, len(k))
			benchHashBytes(h, []byte(k))
			benchHashValue(h, t[k])
		}
	default:
		panic(fmt.Sprintf("unhashable result type %T", v))
	}
}

// generator mirrors the Rust bench's LCG so all layers produce
// byte-identical datasets.
type generator struct {
	state uint64
}

func (g *generator) next() uint64 {
	g.state = g.state*6364136223846793005 + 1442695040888963407
	return g.state >> 11
}

const benchAlphabet = "abcdefghijklmnopqrstuvwxyz012345"

func (g *generator) str(n int) string {
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(benchAlphabet[g.next()%32])
	}
	return b.String()
}

func (g *generator) row3(id int64) (int64, int64, string) {
	return id, int64(g.next()), g.str(16)
}

var wideProps = []string{
	"i0", "i1", "i2", "i3", "i4", "i5", "i6", "i7",
	"f0", "f1", "f2", "f3", "f4",
	"s0", "s1", "s2", "s3",
	"lst", "mp",
}

func (g *generator) wideRow(id int64) map[string]any {
	m := map[string]any{"id": id}
	for i := 0; i < 8; i++ {
		m[fmt.Sprintf("i%d", i)] = int64(g.next())
	}
	for i := 0; i < 5; i++ {
		m[fmt.Sprintf("f%d", i)] = float64(g.next()%100_000) / 7.0
	}
	for i := 0; i < 4; i++ {
		m[fmt.Sprintf("s%d", i)] = g.str(12)
	}
	m["lst"] = []any{
		int64(g.next() % 1000), int64(g.next() % 1000), int64(g.next() % 1000),
		int64(g.next() % 1000), int64(g.next() % 1000),
	}
	m["mp"] = map[string]any{
		"k1": int64(g.next()),
		"k2": g.str(8),
		"k3": g.next()%2 == 0,
	}
	return m
}

func benchOpen(t *testing.T, backend, tag string) *DB {
	t.Helper()
	var opts *OpenOptions
	var path string
	switch backend {
	case "memory":
		opts = &OpenOptions{Memory: true}
		path = ":memory:"
	case "file":
		dir, err := os.MkdirTemp("", "hawdb_bench_")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		path = dir + "/" + tag + ".hawdb"
		opts = &OpenOptions{}
	default:
		t.Fatalf("unknown backend %q", backend)
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("open %s db: %v", backend, err)
	}
	return db
}

// benchRSS returns the process peak RSS in bytes; ru_maxrss is bytes on
// darwin and kilobytes elsewhere.
func benchRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return ru.Maxrss
	}
	return ru.Maxrss * 1024
}

func benchReport(t *testing.T, layer, backend, name string, n, ops int64, wall time.Duration, checksum, rows, values uint64, rssStart int64) {
	rss := benchRSS()
	fmt.Printf("bench,layer=%s,backend=%s,case=%s,n=%d,wall_ms=%.3f,ops=%d,rows_out=%d,values_out=%d,checksum=%016x,peak_rss_delta_kb=%d\n",
		layer, backend, name, n, float64(wall.Nanoseconds())/1e6, ops, rows, values, checksum, (rss-rssStart)/1024)
}

// benchReportErr records a failed measured call; budget denials are
// part of the contract being measured, not a benchmark failure.
func benchReportErr(layer, backend, name string, n, ops int64, wall time.Duration, err error) {
	fmt.Printf("bench,layer=%s,backend=%s,case=%s,n=%d,wall_ms=%.3f,ops=%d,status=error,error=%q\n",
		layer, backend, name, n, float64(wall.Nanoseconds())/1e6, ops, err)
}

// wireLayer names the reported layer for a wire choice: "" is the
// JSON path ("go"), "bin" the binary envelope ("go_bin").
func wireLayer(wire string) string {
	if wire == "bin" {
		return "go_bin"
	}
	return "go"
}

// benchSQL / benchQuery route a statement through the JSON or the
// binary result envelope; the binary path needs a library built with
// the hawdb_query_ex export.
func benchSQL(db *DB, wire, sql string, params ...any) (*Result, error) {
	if wire == "bin" {
		return db.queryBinary(true, sql, params)
	}
	return db.QuerySQL(sql, params...)
}

func benchQuery(db *DB, wire, text string, params map[string]any) (*Result, error) {
	if wire == "bin" {
		return db.queryBinary(false, text, params)
	}
	return db.Query(text, params)
}

func benchChecksumResult(res *Result) (uint64, uint64, uint64) {
	h := uint64(fnvOffset)
	var rows, values uint64
	for _, row := range res.Rows {
		rows++
		values += uint64(len(row))
		for _, v := range row {
			benchHashValue(&h, v)
		}
	}
	return h, rows, values
}

const benchSelectCols = "SELECT id, v, s FROM bench_t"

func benchSetupTable(t *testing.T, db *DB) {
	t.Helper()
	if _, err := db.QuerySQL("CREATE TABLE bench_t (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// benchChunk mirrors the Rust bench's BULK_CHUNK: larger batches trip
// the engine's 16 MiB per-transaction write-set estimate.
const benchChunk = 5000

// benchSeed inserts n generated rows in benchChunk-sized statements and
// returns the statement count.
func benchSeed(t *testing.T, db *DB, wire string, g *generator, n int64) int64 {
	t.Helper()
	var b strings.Builder
	var ops int64
	for start := int64(0); start < n; start += benchChunk {
		b.Reset()
		end := start + benchChunk
		if end > n {
			end = n
		}
		for id := start; id < end; id++ {
			rid, v, s := g.row3(id)
			if id > start {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "(%d,%d,'%s')", rid, v, strings.ReplaceAll(s, "'", "''"))
		}
		if _, err := benchSQL(db, wire,
			"INSERT INTO bench_t (id, v, s) VALUES "+b.String(),
		); err != nil {
			t.Fatalf("seed insert chunk %d: %v", start, err)
		}
		ops++
	}
	return ops
}

func benchFill(t *testing.T, backend, wire string, n int64) {
	db := benchOpen(t, backend, "fill_"+wire)
	defer db.Close()
	benchSetupTable(t, db)
	rssStart := benchRSS()
	g := &generator{state: 0x5eed}
	var checksum uint64 = fnvOffset
	start := time.Now()
	for id := int64(0); id < n; id++ {
		rid, v, s := g.row3(id)
		if _, err := benchSQL(db, wire,
			"INSERT INTO bench_t (id, v, s) VALUES ($1, $2, $3)", rid, v, s,
		); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	benchReport(t, wireLayer(wire), backend, "fill", n, n, time.Since(start), checksum, uint64(n), 0, rssStart)
}

func benchFillBulk(t *testing.T, backend, wire string, n int64) {
	db := benchOpen(t, backend, "fill_bulk_"+wire)
	defer db.Close()
	benchSetupTable(t, db)
	rssStart := benchRSS()
	g := &generator{state: 0x5eed}
	start := time.Now()
	ops := benchSeed(t, db, wire, g, n)
	benchReport(t, wireLayer(wire), backend, "fill_bulk", n, ops, time.Since(start), fnvOffset, uint64(n), 0, rssStart)
}

func benchSelect(t *testing.T, backend, wire string, n int64) {
	db := benchOpen(t, backend, "select_"+wire)
	defer db.Close()
	benchSetupTable(t, db)
	g := &generator{state: 0x5eed}
	benchSeed(t, db, wire, g, n)
	// Warm pass; a budget denial is fine — the measured call reports it.
	benchSQL(db, wire, benchSelectCols)
	rssStart := benchRSS()
	start := time.Now()
	res, err := benchSQL(db, wire, benchSelectCols)
	wall := time.Since(start)
	if err != nil {
		benchReportErr(wireLayer(wire), backend, "select", n, 1, wall, err)
		return
	}
	checksum, rows, values := benchChecksumResult(res)
	benchReport(t, wireLayer(wire), backend, "select", n, 1, wall, checksum, rows, values, rssStart)
}

func benchPoint(t *testing.T, backend, wire string, n int64) {
	db := benchOpen(t, backend, "point_"+wire)
	defer db.Close()
	benchSetupTable(t, db)
	g := &generator{state: 0x5eed}
	benchSeed(t, db, wire, g, n)
	if _, err := benchSQL(db, wire, "SELECT id, v, s FROM bench_t WHERE id = $1", int64(0)); err != nil {
		t.Fatalf("warm point: %v", err)
	}
	rssStart := benchRSS()
	var checksum uint64 = fnvOffset
	var rows, values uint64
	start := time.Now()
	for id := int64(0); id < n; id++ {
		res, err := benchSQL(db, wire, "SELECT id, v, s FROM bench_t WHERE id = $1", id)
		if err != nil {
			t.Fatalf("point %d: %v", id, err)
		}
		h, r, v := benchChecksumResult(res)
		checksum ^= h
		rows += r
		values += v
	}
	benchReport(t, wireLayer(wire), backend, "point", n, n, time.Since(start), checksum, rows, values, rssStart)
}

func benchWide(t *testing.T, backend, wire string, n int64) {
	db := benchOpen(t, backend, "wide_"+wire)
	defer db.Close()
	g := &generator{state: 0x1eaf}
	rows := make([]any, n)
	for id := int64(0); id < n; id++ {
		rows[id] = g.wideRow(id)
	}
	var create strings.Builder
	create.WriteString("UNWIND $rows AS row CREATE (w:Wide {id: row.id")
	for _, name := range wideProps {
		fmt.Fprintf(&create, ", %s: row.%s", name, name)
	}
	create.WriteString("})")
	for start := int64(0); start < n; start += benchChunk {
		end := start + benchChunk
		if end > n {
			end = n
		}
		if _, err := benchQuery(db, wire, create.String(), map[string]any{"rows": rows[start:end]}); err != nil {
			t.Fatalf("wide seed chunk %d: %v", start, err)
		}
	}
	const wideSelect = "MATCH (w:Wide) RETURN w.id, w.i0, w.i1, w.i2, w.i3, w.i4, w.i5, " +
		"w.i6, w.i7, w.f0, w.f1, w.f2, w.f3, w.f4, w.s0, w.s1, w.s2, w.s3, w.lst, w.mp"
	// Warm pass; a budget denial is fine — the measured call reports it.
	benchQuery(db, wire, wideSelect, nil)
	rssStart := benchRSS()
	start := time.Now()
	res, err := benchQuery(db, wire, wideSelect, nil)
	wall := time.Since(start)
	if err != nil {
		benchReportErr(wireLayer(wire), backend, "wide", n, 1, wall, err)
		return
	}
	if dir := os.Getenv("HAWDB_BENCH_DUMP"); dir != "" {
		if data, merr := json.Marshal(res); merr == nil {
			os.WriteFile(dir+"/wide_go"+wire+".json", data, 0o644)
		}
	}
	checksum, outRows, values := benchChecksumResult(res)
	benchReport(t, wireLayer(wire), backend, "wide", n, 1, wall, checksum, outRows, values, rssStart)
}

func TestBoundaryBench(t *testing.T) {
	if os.Getenv("HAWDB_BENCH") == "" {
		t.Skip("set HAWDB_BENCH=1 to run the host-boundary benchmark")
	}
	sizes := []int64{1000}
	if v := os.Getenv("HAWDB_BENCH_SIZES"); v != "" {
		sizes = nil
		for _, s := range strings.Split(v, ",") {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				t.Fatalf("HAWDB_BENCH_SIZES: %v", err)
			}
			sizes = append(sizes, n)
		}
	}
	backends := []string{"memory"}
	if v := os.Getenv("HAWDB_BENCH_BACKENDS"); v != "" {
		backends = strings.Split(v, ",")
	}
	cases := []string{"fill", "fill_bulk", "select", "point", "wide"}
	if v := os.Getenv("HAWDB_BENCH_CASES"); v != "" {
		cases = strings.Split(v, ",")
	}
	wires := []string{""}
	if v := os.Getenv("HAWDB_BENCH_WIRE"); v != "" {
		wires = nil
		for _, w := range strings.Split(v, ",") {
			switch w {
			case "json":
				wires = append(wires, "")
			case "bin":
				wires = append(wires, "bin")
			default:
				t.Fatalf("HAWDB_BENCH_WIRE: unknown wire %q", w)
			}
		}
	}
	for _, backend := range backends {
		for _, n := range sizes {
			for _, wire := range wires {
				for _, name := range cases {
					switch name {
					case "fill":
						benchFill(t, backend, wire, n)
					case "fill_bulk":
						benchFillBulk(t, backend, wire, n)
					case "select":
						benchSelect(t, backend, wire, n)
					case "point":
						benchPoint(t, backend, wire, n)
					case "wide":
						benchWide(t, backend, wire, n)
					default:
						t.Fatalf("unknown case %q", name)
					}
				}
			}
		}
	}
}
