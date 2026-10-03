# Read Snapshot in Datalevin

The database object returned by `(d/db conn)` is a reference to the database,
not a database value. Each read of it uses its own LMDB read transaction and
sees what is committed at that moment. Two reads can therefore see two
different states when a transaction commits in between, and so can two index
scans of the same query.

A read snapshot is a database object that keeps reading the state that was
committed when the snapshot was taken, for as long as it is open. Use it when
several reads have to agree with each other, or when one long read must not be
affected by concurrent writes.

## Usage

`with-read-snapshot` takes a snapshot of the database of a connection, binds it
to a name, and closes it when the body is done:

```clojure
(require '[datalevin.core :as d])

(d/with-read-snapshot [db conn]
  {:revision (:max-tx db)
   :total    (d/q '[:find (count ?e) . :where [?e :order/id]] db)
   :open     (d/q '[:find [(pull ?e [:order/id :order/total]) ...]
                    :where [?e :order/status :open]]
                  db)})
```

A snapshot that is kept across several calls is opened and closed explicitly:

```clojure
(def snapshot (d/open-read-snapshot conn))

(d/read-snapshot? snapshot) ;; => true
(d/pull snapshot '[*] [:order/id 42])
(d/datoms snapshot :ave :order/status :open)

(d/close-read-snapshot snapshot)
```

The snapshot is given to the functions that read a database object: `q`,
`pull`, `pull-many`, `entity`, `datoms`, `seek-datoms`, `rseek-datoms`,
`search-datoms`, `count-datoms`, `index-range`, `explain`, the prepared reads
and so on. These return what they would have returned at the time the snapshot
was taken:

* The data. Later transactions are not seen, whether they add, change or
  retract.
* The schema. `(d/schema snapshot)` is the schema of that state. Attributes
  that are added, renamed, deleted or given another value type later do not
  change how the snapshot reads its data.
* The revision. `(:max-tx snapshot)` is the id of the last transaction in the
  snapshot, and `(:max-eid snapshot)` its largest entity id. Both are read by
  the same LMDB transaction as the data, so they can be used to identify the
  state that a result was computed from.

The KV handle returned by `(d/datalog-kv snapshot)` reads the same state.

## What a snapshot costs

A snapshot holds one LMDB read transaction until it is closed. Like any LMDB
reader, it does not block writers, and writers do not block it. It does have
these costs:

* It occupies one of the reader slots of the database, see `:max-readers` of
  `open-kv`. When all slots are taken, `open-read-snapshot` throws an exception
  with `{:type :lmdb/read-transaction}` in its `ex-data`.
* LMDB cannot reuse the pages of the data that is replaced after the snapshot
  was taken. The database file grows while a snapshot is kept open on a
  database that is being written to, so close snapshots as soon as they are no
  longer needed. A snapshot that is not closed stays open until its connection
  is closed.
* It bypasses the caches that belong to the latest state of the database. It
  is not served cached query results or index lookups, and does not add to
  them, so that neither side sees the data of the other. Its queries are also
  planned each time. The same query can therefore take longer on a snapshot
  than on the database object of the connection.

A snapshot can be read by any thread, including virtual threads, and it can be
closed by a thread other than the one that opened it. As it has a single LMDB
transaction, one thread reads it at a time. The reads of other threads wait for
their turn, and a query on a snapshot runs in the calling thread instead of
being spread over several threads.

A snapshot stays valid when a transaction grows the memory map of the
database.

## What a snapshot does not do

A snapshot is read-only. Writing through it, e.g. with `db-with` or through
its KV handle, and creating a connection from it with `conn-from-db`, throw an
exception with `{:error :read-snapshot/read-only}`. So does `copy` with its KV
handle, as a copy is made of the latest state.

The search functions of the secondary indices, `fulltext`, `fulltext-datoms`,
`vec-neighbors`, `embedding-neighbors` and `idoc-match`, throw an exception
with `{:error :read-snapshot/unsupported}` and the index as `:reason`. These
indices keep part of their state in memory and are not committed together with
the datoms, so they always search the latest state. The datoms of the
attributes that have such indices are read like any other datom.

`open-read-snapshot` throws an exception with `{:error
:read-snapshot/unsupported}` and one of these as `:reason`:

| `:reason` | Meaning |
|----|----|
| `:remote` | The connection is to a Datalevin server. |
| `:ha` | The database is a HA database. The LMDB state of one node is not necessarily the committed state of the cluster. |
| `:transaction` | The connection is the one inside `with-transaction`. Take the snapshot from the original connection instead, it then sees what was committed before the transaction. |
| `:tls-readers` | The database is opened without the `:notls` env flag, so its reader slots belong to threads. `:notls` is among the default flags. |

Databases in [WAL mode](wal.md) are supported, as their transactions are
committed to LMDB when they are acknowledged.

## Closing

`close-read-snapshot` releases the LMDB read transaction. Closing a snapshot
that is already closed does nothing. `close-db` on a snapshot does the same and
leaves the connection open.

Reading from a closed snapshot throws an exception with `{:error
:read-snapshot/closed}`. This includes the lazy attribute lookups of an
`entity` that was obtained from the snapshot, so realize what is needed before
closing.

Closing a snapshot waits for a read of it that is in progress in another
thread. From inside a read of the snapshot itself, e.g. in a query predicate,
it throws an exception with `{:error :read-snapshot/in-use}`.

Closing the connection closes the snapshots that are still open, after which
they behave like any closed snapshot.
