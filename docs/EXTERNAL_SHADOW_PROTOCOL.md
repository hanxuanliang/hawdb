# External Shadow Protocol

Skein uses the external shadow protocol to compare the embedded engine with a
previous local graph wrapper during Nowledge migration gates. The protocol is a
line-delimited JSON request/response stream over child-process stdin/stdout.

This protocol is intentionally small. It covers only the compatibility harness
surface that Nowledge needs for cutover: parameterized Cypher execution, grouped
session execution, and projected graph checks.

## Transport

The parent process starts the shadow engine with piped stdin and stdout. Each
request is one UTF-8 JSON object followed by a newline. Each response must be
one UTF-8 JSON object followed by a newline.

The child process must not write logs or progress messages to stdout. Diagnostic
output belongs on stderr; malformed stdout is treated as a protocol error and
Skein includes a bounded stdout line tail in the error for local debugging.

`request_id` is a monotonically increasing per-process identifier assigned by
Skein. It matches the external shadow trace `sequence` value for the same
request.

The child process must keep its graph state for the lifetime of the process.
Skein sends fixture setup statements and checks to the same process so the
shadow engine can model an embedded database instance.

Every request includes:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "read title",
    "phase": "statement"
  }
}
```

Unknown top-level fields must be ignored by compatible shadow engines. A shadow
engine should reject unsupported protocol versions with an `execution` error.
`context` is diagnostic metadata for wrapper logs and traces. `phase` is one of
`fixture_setup`, `check_setup`, `statement`, `session`, `effect`, or
`project_graph`; `check` is `null` for fixture-wide setup requests.

Responses may include a top-level `request_id` echo. The echo is optional for
backward compatibility, but when present it must match the request `request_id`.
Skein rejects mismatched response identifiers as an `execution` error because
they indicate a stale, reordered, or misrouted shadow response.

## Values

Cypher parameters and result rows use JSON values:

| JSON value | Skein value |
| --- | --- |
| `null` | `Null` |
| `true` or `false` | `Bool` |
| integer number | `Int` |
| floating-point number | `Float` |
| string | `String` |
| array | `List` |
| object | `Map` |

Rows are JSON objects keyed by projected column name. Relationship and node
identities used by projected graph output are unsigned integer identifiers from
the corresponding engine.

## `execute`

`execute` runs one Cypher statement against the shadow engine.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute",
  "role": "read",
  "access": "read",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "read title",
    "phase": "statement"
  },
  "cypher": "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
  "parameters": {
    "id": 1
  }
}
```

Success response:

```json
{
  "ok": {
    "rows": [
      {
        "title": "Graph foundations"
      }
    ]
  }
}
```

Mutation statements should return the same rows the wrapper would expose for
the Cypher statement. For count-only mutation fixtures, an empty row object can
represent one affected row.

## `execute_session`

`execute_session` runs a sequence of Cypher statements inside one logical
session. This is used when a check needs setup, the main statement, and effect
verification to observe the same mutable wrapper state.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "execute_session",
  "access": "mutation",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "update memory title",
    "phase": "session"
  },
  "statements": [
    {
      "cypher": "CREATE (:Memory {id: 1, title: 'Old'})",
      "role": "statement",
      "access": "mutation",
      "parameters": {}
    },
    {
      "cypher": "MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'",
      "role": "statement",
      "access": "mutation",
      "parameters": {}
    },
    {
      "cypher": "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title",
      "role": "statement",
      "access": "read",
      "parameters": {}
    }
  ]
}
```

Success response:

```json
{
  "ok": {
    "outputs": [
      {
        "rows": []
      },
      {
        "rows": [
          {}
        ]
      },
      {
        "rows": [
          {
            "title": "New"
          }
        ]
      }
    ]
  }
}
```

The `outputs` array must have the same length and order as the request
`statements` array. The top-level `access` is `mutation` if any statement in
the session may write; otherwise it is `read`. Per-statement `access` is
advisory but stable: previous-wrapper adapters should use `read` for read-only
Kuzu/Ladybug APIs and `mutation` for serialized write paths. `role` is a
compatibility-harness context label.

## `project_graph`

`project_graph` asks the shadow engine for the projected graph metadata needed
by the compatibility harness.

Request:

```json
{
  "protocol_version": 1,
  "request_id": 1,
  "op": "project_graph",
  "context": {
    "fixture": "nowledge-memory-core",
    "check": "mentions projection",
    "phase": "project_graph"
  },
  "rel_type": "MENTIONS",
  "expected_incoming_nodes": [1],
  "include_communities": false,
  "include_hierarchical_communities": false
}
```

Success response:

```json
{
  "ok": {
    "node_count": 2,
    "edge_count": 1,
    "incoming": [
      [1, [0]]
    ],
    "communities": [],
    "hierarchical_communities": [],
    "page_rank_scores": [
      [0, 0.3508773619358619],
      [1, 0.649122638064138]
    ],
    "page_rank_top_node": 1
  }
}
```

`incoming` contains `[node_id, [source_node_id, ...]]` tuples for the requested
`expected_incoming_nodes`. `communities` contains `[node_id, community_id]`
tuples. `hierarchical_communities` contains
`[level, node_id, community_id]` tuples. `page_rank_scores` contains
`[node_id, score]` tuples in deterministic order.

If the previous wrapper cannot expose projected graph metadata, it may respond
with:

```json
{
  "primary_only": true
}
```

The migration gate treats primary-only projected graph checks as blockers by
default.

## Errors

Any operation can return an error:

```json
{
  "error": {
    "class": "semantic",
    "message": "missing parameter: id"
  }
}
```

`class` must be one of:

- `parse`
- `semantic`
- `storage`
- `execution`

Unknown classes are treated as `execution`. The message is included in the
Skein-side error with the shadow engine name.

## Gate Command

The current migration-gate entry point is:

```text
skein nowledge-cypher-migration-gate [--require-ready] [--allow-self-shadow] [--shadow-trace <path>] [--shadow-timeout-ms <ms>] <root> <shadow-name> <program> [args...]
```

It scans the Nowledge source tree, runs the public Nowledge compatibility
fixture through the external shadow process, prints a JSON migration-gate bundle,
and exits with an error when `--require-ready` is set and the final gate is
blocked.

`--require-ready` requires a previous-wrapper shadow by default. `skein-shadow-self`
is allowed only when `--allow-self-shadow` is passed, and that flag is intended
for protocol and CI smoke tests, not cutover evidence.

`--shadow-trace <path>` writes a JSON-lines transcript of the external shadow
conversation. Each line contains `sequence`, `event`, and `payload`; `event` is
`request`, `response`, or `error`. Error events include the failure message and
the current stderr tail when available. The transcript is intended for
previous-wrapper parity debugging and should be treated as local diagnostic
output because Cypher parameters may contain graph data.

Each request waits up to 30000 ms for one stdout response line by default.
`--shadow-timeout-ms <ms>` overrides that per-request timeout. A timeout kills
the shadow process and fails the gate with an `execution` error instead of
letting CI or local cutover runs hang indefinitely.
