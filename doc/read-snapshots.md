# Explicit embedded read snapshots

Datalevin's ordinary `(d/db conn)` result is a live database object, not a
Datomic-style immutable database value. Reusing that object does not pin one
LMDB read transaction. Use the explicit read-snapshot API when several reads
must observe one native state.

```clojure
(require '[datalevin.core :as d])

(with-open [snapshot (d/open-read-snapshot conn)]
  (d/with-read-snapshot
   snapshot
   (fn [db]
     {:revision (:max-tx (d/read-snapshot-revision-info snapshot))
      :rows (vec (d/datoms db :ave :app/id))})))
```

`open-read-snapshot` supports only local embedded Datalog connections. It
opens a fresh explicit read-only LMDB transaction that is independent of the
ambient thread-local reader. Remote/server, WAL, HA, and historical revision
selection are separate features; an explicit read snapshot does not turn any
of them into a historical database API.

The returned handle owns native reader state and implements `AutoCloseable`.
Close it on the acquiring thread with `close-read-snapshot!` or `with-open`.
Close is idempotent. Connection/environment close is rejected while one of its
explicit snapshots is active, so callers can close the snapshot and retry.

Snapshots are confined to their acquiring platform thread. Acquisition from a
virtual thread, cross-thread access or close, use after close, and conflicting
nested snapshots fail with typed errors before native access. Nested use of the
same snapshot is allowed. The connection must outlive all of its snapshots.

All access to the snapshot database must occur synchronously inside
`with-read-snapshot`. Returning the database and accessing it later fails
closed. Bounded Datalevin reads returned by the callback are eager, but callers
must explicitly realize any application-created lazy transformation before the
callback returns.

Revision bounds are captured eagerly under the same reader as the data:

- persisted `:max-tx`; and
- coherent `:max-eid`.

Physical decoding state is materialized lazily, at most once, through that
already-owned reader when the first Datalog read or `read-snapshot-info` call
needs it:

- physical schema and reverse schema;
- attribute-ID mappings; and
- persisted decoding/store options.

Call `read-snapshot-revision-info` when only snapshot identity is needed. It
returns the captured `:max-tx` and `:max-eid` map without reading, copying, or
comparing the physical schema metadata returned by `read-snapshot-info`.

The snapshot uses private schema/cache state and bypasses Datalevin's
directory-global Datalog, query-result, and optimizer caches for its lifetime.
`read-snapshot-cache-bypass?` is a runtime guard for that property.

`read-snapshot-capabilities` reports whether a connection is a supported
embedded store and exposes its WAL, HA, and LMDB environment profile without
acquiring a reader. `active-read-snapshot-info` reports the process-wide active
count and oldest reader age for health checks. Reader-slot exhaustion, a closed
environment, and other recognized native acquisition failures are classified
as `:datalevin/read-snapshot-acquisition-failed`. Recognized native failures
while a snapshot-bound read is executing are classified as
`:datalevin/read-snapshot-native-read-failed`. Both errors include
`:operation` and a bounded `:reason` such as `:reader-slots-exhausted`,
`:environment-closed`, `:map-resized`, `:map-full`, `:disk-full`, or
`:native-read-failure`; neither exposes the database directory. Exceptions
thrown by application callback code are not reclassified, even when their
message or exception data mimics one of those native failure signatures;
classification additionally requires a native binding origin.

The API is intentionally current-snapshot-only. It provides no lookup by old
transaction number, no durable-history guarantee, and no promise that a backup
or WAL snapshot can reconstruct the handle. Applications that need historical
selection must use a separately specified and certified history mechanism.

Changes to the snapshot API, native Datalevin library, JDK, or platform require
rerunning acquisition, concurrent-write isolation, schema coherence, eager
read, cache-isolation, failure-cleanup, and close-order certification before
the combination is declared supported.

The source contract suite uses real local stores for map growth, reader-slot
exhaustion, connection close, and closed-environment acquisition. It uses a
test-only dynamic fault injector to reproduce deterministic LMDB map-full,
disk-full, and native page-read signatures. Those injected cases certify error
classification, reader accounting, retryability, and cleanup; they do not
claim to reproduce operating-system quota exhaustion or LMDB page corruption.
