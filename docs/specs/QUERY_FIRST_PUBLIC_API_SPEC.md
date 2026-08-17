# Query-First Public API Specification

## Status

This specification defines the release-facing embedded API boundary. It is
normative for production builds.

## Public query boundary

Application graph behavior MUST be expressed as parameterized Cypher and
executed through `Database`, `DatabaseReadTransaction`,
`DatabaseTransaction`, `NowledgeGraphAdapter`, or the bounded
`NowledgeMemGraph` query surface. Relational behavior MUST be expressed as
PostgreSQL-dialect SQL through the corresponding SQL entry points.

Every application-owned read statement MUST have explicit row and payload
budgets. Multiple distinct read phases SHOULD remain separate named
statements. The host MAY normalize requests, account for budgets across
statements, and shape compatibility responses, but MUST NOT reimplement graph
scan, join, filter, sort, aggregate, or traversal semantics.

Application mutations MUST use parameterized Cypher inside
`DatabaseTransaction` when more than one statement must publish atomically.
The transaction owns read-your-own-writes behavior and the single canonical
WAL publication boundary.

## Removed business facades

Release builds MUST NOT expose application-specific entity, relationship,
Memory, Source, Skill, Thread, Label, Community, or AugmentationJob CRUD batch
methods. Release builds also MUST NOT expose `read_graph_*` route methods or
their route response DTOs.

The old adapters may remain compiled only for unit-test regression fixtures
while their semantic coverage is migrated to direct query tests. They are not
part of the release API and MUST NOT be used by host integration code.

## Stable typed boundaries

A typed API remains appropriate only when a stable kernel contract coordinates
behavior that cannot be represented safely by one query language statement.
The retained categories are:

- database open, configuration, sessions, and transactions;
- bounded streaming and query reports;
- WAL, checkpoint, recovery, doctor, and storage inspection;
- schema migration and maintenance orchestration;
- search projection, changefeed, freshness, and rebuild boundaries;
- bounded unified knowledge retrieval over canonical graph identities;
- Skein Lightning import and canonical snapshot boundaries;
- QoS admission, readiness, qualification, and telemetry evidence.

These APIs MUST remain route-neutral. A new REST route, scheduler operation, or
business entity is not sufficient justification for a new typed database
method.

## Compatibility and formal-model impact

Skein has no released public compatibility obligation for the removed business
facades. This is an intentional source-breaking cleanup before the first
release.

The cleanup does not change transaction, WAL, checkpoint, MVCC, lock, recovery,
or query execution semantics. Existing TLA+ safety and liveness obligations
therefore remain unchanged. Any future typed boundary that introduces a new
publication, recovery, concurrency, or admission state transition MUST update
the corresponding specification and formal model before release.

## Verification

The release build MUST compile without the test-only business type modules.
Unit tests MAY compile the legacy fixtures until equivalent parameterized query
tests replace them. Required validation is:

```console
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
bazel test --test_output=errors //...
```
