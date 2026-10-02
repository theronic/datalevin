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
does not admit them.

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
`:attributes` name the frozen attributes it was rejected for. A rejected
schema map writes none of its attribute definitions.

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

## Commit-generation continuity

After obtaining LMDB's cross-process writer lock, every Datalog commit reads
the persisted `:max-tx` and compares it with the open store's in-memory value.
A mismatch aborts with
`:datalevin/max-tx-continuity-violation` and marks that environment unusable
for subsequent writes until it is reopened. This detects a foreign process
that advanced the generation from a separate in-memory allocator. The
committing generation, every commit-generation datom, and the persisted
post-commit `:max-tx` are required to be equal.

## Entry points and boundary

The check covers synchronous and asynchronous transactions, blocking
transactions, explicit `with-transaction`, direct `transact-tx-data`, and the
blind map fast path. Speculative `with` results do not commit and are exempt.
Transactions that touch no guarded attribute remain ordinary Datalevin
transactions.

The boundary assumes all Datalog writes use this fork. Raw KV mutation of the
datom/meta DBIs, direct file modification, and opening the directory with an
upstream binary that does not implement the policy are outside the contract.
LMDB `:nolock` is incompatible with the continuity guarantee.
