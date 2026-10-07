.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0

================================
Proposal: Partitioned Bulk Ingest
================================

.. note::

   Status: draft.  Targets ADBC API revision 1.2.0.

Motivation
==========

Today ADBC supports two ingest shapes:

- **Single-writer bulk ingest** — one connection, one statement, one
  ``ArrowArrayStream``, one transaction.  Good for loading from a single
  process; useless for distributed writers.
- **Per-row binding** — slower, also single-connection.

Two real workloads do not fit:

1. **Distributed-writer to RDBMS.**  A Spark/Flink/Beam job runs N
   executors, each producing a partition of the output.  Today each
   executor opens its own ADBC connection and runs its own bulk
   ingest, but the result is *not atomic*: there is no commit point at
   which all N partitions become visible together.  Workarounds
   (per-job staging tables, ad-hoc swap SQL) are database-specific and
   leak into application code.

2. **Distributed-writer to table-format catalogs (Apache Iceberg,
   Delta Lake).**  These formats are *designed* for distributed
   writes: many workers write data files in parallel, and a single
   commit step writes a snapshot/manifest in the catalog or
   transaction log.  ADBC currently has no way to expose this shape.
   A driver author who wants to write to Iceberg today has to pick
   between (a) routing all writes through one process (defeats the
   point) or (b) inventing a private API.

The unifying observation is that both workloads need the same shape:
**coordinator decides what to ingest, workers write partitions in
parallel, coordinator commits or aborts atomically**.  That is the
mirror image of partitioned read (``ExecutePartitions`` /
``ReadPartition``), which ADBC already supports.

Goals
-----

- Allow a coordinator to start an ingest, ship an opaque token to N
  workers (possibly in different processes or hosts), have each
  worker independently write a partition over its own connection, and
  finally commit (or abort) atomically from the coordinator.
- Be implementable by both RDBMS drivers (via per-worker staging
  tables) and table-format drivers (via per-worker data files +
  catalog commit) without forcing either model on the other.
- Survive lost worker writes, dropped receipts, and coordinator
  restarts without leaving silent data corruption.
- Keep the per-driver cost low: most of the ingest plumbing
  (CREATE TABLE, COPY, schema mapping) is reused from existing
  single-writer ingest.

Non-goals
---------

- Schema evolution mid-ingest.  Schema is fixed when ``Begin`` is
  called; changing it requires starting a new ingest.
- Cross-driver atomicity (writing to two databases in one commit).
- Defining how a distributed engine (Spark, Flink) ships handles and
  receipts between processes.  That is the application's problem;
  the API guarantees only that handles and receipts are opaque,
  serializable byte strings.
- General idempotency of ``Complete``.  If the coordinator
  double-commits (calls ``Complete`` again on a handle that it knows
  was completed successfully) the second call is undefined.
  Repeating a ``Complete`` that *failed* as retryable or with an
  unknown outcome is well-defined; see "Failed ``Complete``" below.

Design overview
===============

Three new operations on ``AdbcStatement``, plus an ``Abort``:

::

   coordinator: Begin(schema)                          → handle
   workers:     Write(handle, stream)                 → receipt
                Write(handle, stream)                 → receipt
                ...
   coordinator: Complete(handle, [receipt, receipt, …]) → rows_affected
   (or)         Abort(handle, [receipts...])

The handle and each receipt are **opaque, serializable byte
strings**.  This is the same shape as the existing partitioned-read
side, where ``AdbcStatementExecutePartitions`` returns opaque
``AdbcPartitions`` byte strings that can be shipped to workers and
passed to ``AdbcConnectionReadPartition`` over a different
connection.

API surface
-----------

The target table, mode, and optional catalog/schema are set via the
existing ``ADBC_INGEST_OPTION_*`` statement options before calling
``Begin``, exactly as for single-writer bulk ingest.  The handle
captures them, so they do not need to be set again on the statements
used for ``Write``, ``Complete``, and ``Abort``.

C declarations (see ``adbc.h`` for full doc comments):

.. code-block:: c

   struct AdbcSerializableHandle {
     size_t length;
     const uint8_t* bytes;
     void* private_data;
     void (*release)(struct AdbcSerializableHandle*);
   };

   AdbcStatementBeginIngestPartitions(
       stmt, schema, *out_handle, *error);

   AdbcStatementWriteIngestPartition(
       stmt, handle_bytes, handle_len, *data_stream,
       *out_receipt, *error);

   AdbcStatementCompleteIngestPartitions(
       stmt, handle_bytes, handle_len, num_receipts, receipts,
       receipt_lens, *rows_affected, *outcome, *error);

   AdbcStatementAbortIngestPartitions(
       stmt, handle_bytes, handle_len, num_receipts, receipts,
       receipt_lens, *error);

The asymmetry — outputs are driver-owned structs, inputs are raw
``bytes + len`` — is deliberate and matches the read side: the bytes
are the part the caller serializes for transport, while the structs
hold driver-owned memory that callers release locally.

Driver-side semantics
---------------------

- **Begin** validates options, performs whatever setup the driver
  requires for writes to proceed (e.g., creating the target table for
  ``create``/``replace``/``create_append`` modes, reserving a
  transaction snapshot, allocating an object-store prefix), and returns
  a handle that encodes the state needed to scope subsequent writes.
- **Write** takes a handle and a stream, writes the partition into
  driver-private staging (a per-write staging table, a per-write
  object-store path), and returns a receipt encoding what was
  written (staging name, file paths, row count, statistics, ...).
  Each ``Write`` call must produce output that can be committed or
  discarded *independently* — no shared state across concurrent
  writes that would cause duplicate rows on retry.
- **Complete** atomically promotes the union of the supplied receipts
  into the target.  Atomic semantics are driver-specific: RDBMS
  drivers swap staging into target in a transaction; table-format
  drivers write a catalog or transaction-log entry referencing the
  data files in the receipts.  After successful commit the handle is
  consumed.  On failure the driver reports through ``outcome``
  whether the ingest is dead, whether the same call can simply be
  repeated, or whether it does not know if the commit took effect
  (see "Failed ``Complete``" below).
- **Abort** discards all writes scoped to the handle.  The driver
  must clean up *every* write under the handle, not just the ones
  named in the supplied receipts (see "Lost receipts" below).

Cross-process flow
------------------

::

   ┌──────────────┐
   │ coordinator  │  Begin(...) ─→ handle
   └──────┬───────┘
          │  copy handle.bytes; ship to workers
          ▼
   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
   │  worker 1    │    │  worker 2    │ …  │  worker N    │
   │ Write(...) →│    │ Write(...) →│    │ Write(...) →│
   │   receipt₁   │    │   receipt₂   │    │   receipt_N  │
   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
          │  copy receipt.bytes; ship back        │
          ▼                  ▼                    ▼
   ┌──────────────────────────────────────────────────────┐
   │ coordinator: Complete(handle, [r₁, r₂, ..., r_N])      │
   └──────────────────────────────────────────────────────┘

Workers may use *different* connections than the coordinator — the
handle is self-contained.  Each party creates a statement on its own
connection to make the call.

Key design decisions
====================

The decisions below were the ones with non-obvious tradeoffs.

1. Opaque handles and receipts
------------------------------

Driver-defined byte strings, no schema imposed by ADBC.  This lets a
Postgres driver encode "staging table prefix + UUID" while an
Iceberg driver encodes "snapshot id + data file paths + column
stats" — without ADBC having to model both.  The cost is that
applications cannot inspect handles or receipts.  Worth it: the only
party that ever needs to interpret them is the driver.

2. Schema is fixed at ``Begin``, not per-``Write``
--------------------------------------------------

For ``create``/``replace``/``create_append`` modes, the driver
issues ``CREATE TABLE`` (or the catalog equivalent) at ``Begin``
time, before any worker writes.  Workers cannot race to "create on
first write" because they are on different machines.  Iceberg/Delta
also need the schema pinned into the transaction snapshot at start.

For ``append`` mode, the schema parameter is optional; if supplied
it is validated against the target so a thousand workers don't all
fail independently with the same schema-mismatch error.

3. Driver-owned output structs (handle, receipt)
-------------------------------------------------

An earlier draft used the ``GetOptionBytes`` two-phase sizing
pattern: caller passes a buffer + capacity, driver reports required
length, caller retries with a larger buffer.  This is correct only
for *idempotent* operations.  ``Begin`` and ``Write`` produce
irrecoverable side effects (``CREATE TABLE``, ``COPY``); a
buffer-too-small failure left the side effects in place but gave the
caller no handle/receipt to pass to ``Abort`` — an unrecoverable
orphan.

The chosen pattern (driver-owned struct with a release callback)
mirrors ``AdbcPartitions`` on the read side, eliminates the orphan
window, and gives drivers a clean place to free internal state.

4. ``Complete`` and ``Abort`` take raw bytes, not structs
-------------------------------------------------------

Symmetric with ``AdbcConnectionReadPartition``, which takes the raw
bytes from a ``partitions[i]`` entry rather than the
``AdbcPartitions`` struct.  Receipts that traveled across processes
arrive as raw bytes; forcing the caller to wrap them in
``AdbcSerializableHandle`` structs (with bogus ``release`` callbacks)
would be friction without benefit.

5. Lost receipts are handled by handle-scoped sweep, not by receipts
--------------------------------------------------------------------

If a worker writes data but its receipt is lost in transit, the
coordinator's receipt list is incomplete.  ``Complete`` will not
include the orphan (correct: only acknowledged writes are
committed).  ``Abort``, however, must clean it up — and ``Abort``
cannot rely on the supplied receipts alone, because the orphan
isn't in them.

The handle therefore must encode enough scope (UUID prefix,
transaction id, object-store path) for the driver to enumerate
*everything* written under it.  Receipts passed to ``Abort`` are an
optimization (fast-path deletion of known writes); the handle is the
authority for cleanup scope.  Drivers that cannot enumerate from
the handle alone cannot correctly implement partitioned ingest.

6. Coordinator may die without calling ``Complete`` or ``Abort``
--------------------------------------------------------------

The handle is opaque to the driver outside of ``Write``, so the
driver has no built-in liveness signal.  Recommended (not required)
behaviors:

- Drivers may TTL or background-GC handle-scoped writes.
- Callers may persist the handle bytes and call ``Abort`` after
  restart to recover.
- Iceberg/Delta drivers can rely on existing orphan-file cleanup
  tooling.

The spec does not mandate any of these; it documents the failure
mode and leaves the policy to drivers.

7. Operations live on ``AdbcStatement``, not ``AdbcConnection``
---------------------------------------------------------------

None of the four operations executes the statement's query, and
``Write``/``Complete``/``Abort`` take everything they need from the
handle, so the connection is the more obvious home (and is where the
read-side ``AdbcConnectionReadPartition`` lives).  An earlier draft
did that, first with the target table and mode as function
parameters and then as connection options.

Both have problems.  Baking the options into the signature
duplicates the existing ``ADBC_INGEST_OPTION_*`` keys and leaves no
room for driver-specific ingest options.  Setting them as connection
options makes them long-lived, shared state on an object that may be
running unrelated work.  A statement gives the options a natural
scope — set them, call ``Begin``, release the statement — and keeps
partitioned ingest consistent with single-writer bulk ingest, which
is already configured through statement options.

The cost is that the caller must allocate a statement for each step
and that the statement gains operations unrelated to its query: any
query, Substrait plan, or bound data on the statement is ignored and
left unmodified.

8. Failed ``Complete``: an outcome code, not a new status code
--------------------------------------------------------------

A failed ``Complete`` falls into one of three groups that the caller
must be able to tell apart, because the correct reaction to each is
different and the wrong one loses or corrupts data:

- **Failed** (``ADBC_INGEST_COMPLETE_FAILED``).  Nothing was
  promoted and the ingest cannot be completed.  The caller calls
  ``Abort`` and starts over.  Example: the target's schema was
  changed concurrently and no longer matches the one fixed at
  ``Begin``.
- **Retryable** (``ADBC_INGEST_COMPLETE_RETRYABLE``).  Nothing was
  promoted and the staged data is intact; repeating the call with
  the same handle and receipts may succeed.  The motivating case is
  an Iceberg or Delta Lake commit that loses an
  optimistic-concurrency race (Iceberg's ``CommitFailedException``,
  a Delta log version that was already taken).  Rewriting the
  partitions here would waste the whole job.
- **Unknown** (``ADBC_INGEST_COMPLETE_UNKNOWN``).  The driver sent
  the commit but never learned the result — the connection dropped
  while waiting for the catalog, object store, or database to
  acknowledge it.  This is Iceberg's ``CommitStateUnknownException``,
  and the same thing happens to an RDBMS driver that loses its
  connection during ``COMMIT``.  The caller must *not* ``Abort``: if
  the commit did land, a handle-scoped sweep would delete data files
  that the table now references.  The only safe action is to call
  ``Complete`` again and let the driver find out.

To make the last case resolvable, a driver that reports *unknown*
must be able to tell, on a repeated ``Complete``, whether the earlier
attempt took effect, and succeed without promoting the writes twice
if it did.  The handle is the natural key for this: an Iceberg driver
can record a handle-derived id in the snapshot summary and look for
it in the table history, a Delta Lake driver can use the transaction
identifier (``txn`` action) that the format provides for idempotent
writers, and an RDBMS driver that drops its staging tables in the
same transaction as the insert can check whether they still exist.
Drivers that never report *unknown* need none of this.

Drivers must report *failed* or *retryable* only if they know nothing
was promoted, and *unknown* otherwise.

No existing status code means "repeat this exact call", let alone
"don't know".  New status codes (``ADBC_STATUS_CONFLICT``,
``ADBC_STATUS_RETRY``) were considered and rejected: status codes are
global, so a new code could in principle be returned from any
function, and existing callers that do not know it would be affected
for the sake of a single operation.  Instead ``Complete`` has an
``int* outcome`` out parameter taking ``ADBC_INGEST_COMPLETE_*``
values, following the header's convention of an integer plus
``#define`` constants for small value sets (cf.
``ADBC_OBJECT_DEPTH_*``).  Language bindings are free to surface it
idiomatically (a field on the exception, a richer result type)
rather than as an out parameter.

The parameter is required rather than optional.  A caller that
ignored it would have to treat every failure as terminal and call
``Abort``, which is exactly the wrong thing to do for an unknown
outcome.

Reference implementation
========================

A prototype lives in the PostgreSQL driver
(``c/driver/postgresql/ingest_partition.{h,cc}``).  It uses
per-worker ``UNLOGGED`` staging tables of the form
``adbc_stg_<uuid>_<random>``, a single ``BEGIN``/``COMMIT``
wrapping ``INSERT INTO target SELECT cols FROM staging`` for each
receipt, and an ``Abort`` that scans
``information_schema.tables`` for the handle's prefix.  Test
coverage is in ``c/driver/postgresql/partitioned_ingest_test.cc``.
