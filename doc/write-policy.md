# Persisted write policies

Datalevin's maintained embedded fork can enforce an application-defined
write policy at the storage commit boundary. The mechanism is generic; it
does not contain EACL attribute names or authorization logic.

## Contract

A policy has four executable parts:

```clojure
{:guarded-attributes #{:example/content :example/generation}
 :frozen-attributes #{:example/content :example/generation}
 :commit-generation-attributes #{:example/generation}
 :stamp-rules
 [{:when-attribute :example/content
   :stamp-attribute :example/generation
   :stamp-entity :same-entity}]}
```

- A transaction whose fully expanded datoms touch a guarded attribute must
  carry the admission token in `tx-meta` under
  `:datalevin/write-token`.
- Every added or retracted datom matching a stamp rule requires an added
  stamp datom in the same transaction. Supported selectors are
  `:same-entity`, `[:tuple-position n]`, and `[:constant eid]`.
- A commit-generation attribute is cardinality-one `:db.type/long`.
  `:db/current-tx` in its value position materializes to the committing
  store generation without allocating an entity id. Any explicit different
  value is rejected as `:datalevin/stale-generation`.
- Frozen attributes cannot be changed through schema mutation, protected DBI
  clearing or dropping, or re-indexing without the administrative token.

Enforcement runs after transaction-function and `retractEntity` expansion,
but before any datom or secondary-index side effect commits. Consequently,
an indirect retraction cannot hide a guarded datom from the policy.

## Installing and opening

Install a policy after its attributes exist:

```clojure
(let [{:keys [write-token]}
      (d/install-write-policy! conn policy)]
  (d/transact!
   conn tx-data
   {:datalevin/write-token write-token}))
```

The normalized policy is persisted in the metadata DBI. Each environment
open mints a new UUID admission token; tokens do not survive close/reopen.
Calling `install-write-policy!` with the identical persisted policy returns
the current open's token. Replacing an existing policy requires that token in
the optional `tx-meta` argument.

Administrative operations can be admitted synchronously without placing a
dynamic binding around queued transaction work:

```clojure
(d/with-write-policy-token
 conn write-token
 #(d/re-index conn {}))
```

Normal Datalog transactions must use `tx-meta`; the administrative binding
does not admit them. [Re-indexing](#re-indexing) describes what an admitted
re-index keeps.

## What an operation touches

The policy is checked against the attributes an operation touches, not the
attributes it names.

A transaction touches an attribute when its fully expanded datoms include a
datom of that attribute, added or retracted.

A schema operation touches an attribute when applying it would leave the
attribute's effective definition different from the stored one:

- the attribute is created, deleted, or renamed (a rename touches both
  names);
- one of its properties is added, changed, or retracted; or
- it is a composite tuple attribute that has no `:db/tupleTypes` yet and
  gains them by inference because the operation types its last untyped
  source attribute, even though the operation does not name it.

A schema map is a patch, both when it is given at open and when it is given
to `update-schema`: properties it omits are preserved, and `:db/aid` is
ignored. Re-declaring an attribute exactly as stored therefore touches
nothing. Only touched attributes are checked against `:frozen-attributes`,
and only they are written.

```clojure
;; :example/content is frozen and stored as a string.

;; Admitted without the token: :example/content is re-declared unchanged, so
;; only the new :app/note is touched.
(d/create-conn dir {:example/content {:db/valueType :db.type/string}
                    :app/note        {:db/valueType :db.type/string}})

;; Rejected without the token: :example/content would change.
(d/update-schema conn {:example/content {:db/cardinality :db.cardinality/many}
                       :app/note        {:db/valueType :db.type/string}})
```

A rejected schema operation throws `:datalevin/frozen-attribute-write`, whose
`:attributes` name every frozen attribute the operation would have changed.

The whole operation (its schema map, deletions and renames together) is
checked after validation and before its first write. A rejected operation is
therefore not applied in part: no attribute is created, changed, deleted or
renamed, and no stored value is re-encoded for a new value type, even when
the rejection is caught inside a transaction that the caller then commits. A
connection whose schema map is rejected is not opened and keeps no reference
to the store, so it cannot hold the environment open after the other
connections close.

An attribute admitted this way is an ordinary attribute. Admission does not
extend the persisted policy, so the attribute is neither guarded nor frozen
until the application installs a policy that names it.

The first open of a store has no token to present, because that open mints
the token. A schema map given at open can therefore add attributes and
re-declare frozen ones, but cannot change a frozen one. Make such a change
after opening, under `with-write-policy-token`.

Clearing or dropping a protected DBI and re-indexing act on every attribute
at once, so they are treated as touching every frozen attribute. Their
rejection carries the `:operation`, and the `:dbi-name` when there is one,
instead of `:attributes`.

## Bulk loads

`init-db`, `conn-from-datoms`, `fill-db` and loading a dump write their datoms
in batches of `datalevin.constants/*fill-db-batch-size*`, all inside one write
transaction. The policy is enforced on each batch as it is on the datoms of a
transaction. A rejected batch rejects the whole load: none of its batches is
committed, the open store keeps its generation, and a store that `init-db` or
`conn-from-datoms` opened for the load is closed again.

A bulk load has no `tx-meta`, and the administrative binding does not admit
datoms, so a bulk load cannot write a guarded attribute. Guarded datoms are
written by transactions; only the reload of a [re-index](#re-indexing) loads
them in bulk.

`fill-db` on the database of an enclosing `with-transaction` joins that
transaction. A bulk load started on a thread that holds another write
transaction on the same store, such as a caller's `with-transaction-kv`, is
rejected with `:datalevin/nested-write-transaction`: its commit would have to
open a second write transaction, which LMDB never grants to one thread.

## Re-indexing

`re-index` dumps the store, clears it, loads the dump back and returns a new
connection. The operation is admitted once, before it writes anything: it
needs the token when the policy has frozen attributes, and no token otherwise.

The dump is the store's own data, which the policy admitted when it was first
written. Loading it back is therefore not checked against the policy again:

- guarded datoms are loaded back without a token, and stamp rules are not
  applied to them;
- commit-generation datoms keep their stored values;
- frozen attributes are created again as they were dumped, or as the schema
  given to the re-index changes them.

The policy stays installed, and the generation is not restarted: the clear
keeps the persisted `:max-tx` and the reload commits the generations after
it, so no generation is committed twice. The returned connection belongs to a
newly opened environment. It has a new token, and the token that admitted the
re-index is no longer valid.

```clojure
(let [conn  (d/with-write-policy-token
             conn write-token
             #(d/re-index conn {}))
      token (:write-token (d/install-write-policy! conn policy))]
  (d/transact! conn tx-data {:datalevin/write-token token}))
```

A re-index that fails after it cleared the store, for example because the
schema it was given does not fit the stored values, loads the dump back with
the schema and options it was taken with, and then throws. The store holds
the datoms, schema and policy it held before, and its generation has not gone
back. The connection that was passed in is closed.

If the dump cannot be loaded back either, the error is
`:datalevin/re-index-unrestored`. The store is then cleared. `:dump` in the
error data names the file that holds its options, schema and datoms. The file
is moved aside, from `dl-dump` to `dl-dump-<timestamp>` in the store
directory, so that a later re-index does not overwrite it. Loading it by hand
is a bulk load: the store's policy rejects its guarded datoms, and a new
directory accepts it but starts without the policy and from the first
generation.

## Commit-generation continuity

After obtaining LMDB's cross-process writer lock, every Datalog commit reads
the persisted `:max-tx` and compares it with the open store's in-memory value.
A mismatch aborts with
`:datalevin/max-tx-continuity-violation` and marks that environment unusable
for subsequent writes until it is reopened. This detects a foreign process
that advanced the generation from a separate in-memory allocator. The
committing generation, every commit-generation datom, and the persisted
post-commit `:max-tx` are required to be equal.

Each batch of a bulk load is such a commit and takes one generation. The
batches read the persisted `:max-tx` through the transaction they share, so
every batch sees the generations of the batches before it, and the open store
adopts the last of them when that transaction has committed.

The reload of a re-index is the one commit whose commit-generation datoms
differ from the committing generation: they are loaded back with the values
they were stored with.

## Entry points and boundary

The check covers synchronous and asynchronous transactions, blocking
transactions, explicit `with-transaction`, direct `transact-tx-data`, the
blind map fast path, and the batches of a bulk load. Speculative `with`
results do not commit and are exempt. Transactions that touch no guarded
attribute remain ordinary Datalevin transactions.

The boundary assumes all Datalog writes use this fork. Raw KV mutation of the
datom/meta DBIs, direct file modification, and opening the directory with an
upstream binary that does not implement the policy are outside the contract.
LMDB `:nolock` is incompatible with the continuity guarantee.

A dump holds a store's options, schema and datoms. It holds neither the
policy nor the generation, so a dump loaded into another directory creates a
store without a policy whose generation starts anew.

A re-index is not atomic. A process that stops between the clear and the end
of the reload leaves the store cleared, with its data in `dl-dump` in the
store directory.
