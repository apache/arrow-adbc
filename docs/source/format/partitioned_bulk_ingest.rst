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
- Idempotency on the coordinator side.  If the coordinator
  double-commits (calls ``Commit`` twice on the same handle) the
  second call is undefined.

Design overview
===============

Three new operations on ``AdbcConnection``, plus an ``Abort``:

::

   coordinator: Begin(table, mode, schema)            → handle
   workers:     Write(handle, stream)                 → receipt
                Write(handle, stream)                 → receipt
                ...
   coordinator: Commit(handle, [receipt, receipt, …]) → rows_affected
   (or)         Abort(handle, [receipts...])

The handle and each receipt are **opaque, serializable byte
strings**.  This is the same shape as the existing partitioned-read
side, where ``AdbcStatementExecutePartitions`` returns opaque
``AdbcPartitions`` byte strings that can be shipped to workers and
passed to ``AdbcConnectionReadPartition`` over a different
connection.

API surface
-----------

C declarations (see ``adbc.h`` for full doc comments):

.. code-block:: c

   struct AdbcIngestHandle {
     size_t length;
     const uint8_t* bytes;
     void* private_data;
     void (*release)(struct AdbcIngestHandle*);
   };

   struct AdbcIngestReceipt {
     size_t length;
     const uint8_t* bytes;
     void* private_data;
     void (*release)(struct AdbcIngestReceipt*);
   };

   AdbcConnectionBeginIngestPartitions(
       conn, target_catalog, target_db_schema, target_table, mode,
       schema, *out_handle, *error);

   AdbcConnectionWriteIngestPartition(
       conn, handle_bytes, handle_len, *data_stream,
       *out_receipt, *error);

   AdbcConnectionCommitIngestPartitions(
       conn, handle_bytes, handle_len, num_receipts, receipts,
       receipt_lens, *rows_affected, *error);

   AdbcConnectionAbortIngestPartitions(
       conn, handle_bytes, handle_len, num_receipts, receipts,
       receipt_lens, *error);

The asymmetry — outputs are driver-owned structs, inputs are raw
``bytes + len`` — is deliberate and matches the read side: the bytes
are the part the caller serializes for transport, while the structs
hold driver-owned memory that callers release locally.

Driver-side semantics
---------------------

- **Begin** validates options, creates the target table for
  ``create``/``replace``/``create_append`` modes, and returns a handle
  that encodes whatever state the driver needs to scope subsequent
  writes (UUID, target catalog/schema/table, transaction id, object
  store prefix, etc.).
- **Write** takes a handle and a stream, writes the partition into
  driver-private staging (a per-write staging table, a per-write
  object-store path), and returns a receipt encoding what was
  written (staging name, file paths, row count, statistics, ...).
  Each ``Write`` call must produce output that can be committed or
  discarded *independently* — no shared state across concurrent
  writes that would cause duplicate rows on retry.
- **Commit** atomically promotes the union of the supplied receipts
  into the target.  Atomic semantics are driver-specific: RDBMS
  drivers swap staging into target in a transaction; table-format
  drivers write a catalog or transaction-log entry referencing the
  data files in the receipts.  After successful commit the handle is
  consumed.
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
   │ coordinator: Commit(handle, [r₁, r₂, ..., r_N])      │
   └──────────────────────────────────────────────────────┘

Workers may use *different* connections than the coordinator — the
handle is self-contained.

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

3. No caller-supplied partition IDs
-----------------------------------

Earlier drafts gave each ``Write`` a caller-supplied ``partition_id``
for idempotent retry.  Dropped: receipts are the source of truth for
what gets committed, and well-designed drivers write each ``Write``
to a unique location (per-call staging table, per-call data file).
A retried ``Write`` produces a *new* receipt; the original write
becomes orphaned and is collected by ``Abort``.  Caller-supplied IDs
only matter for drivers that share staging across writes — which they
shouldn't.

4. Driver-owned output structs (handle, receipt)
------------------------------------------------

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

5. ``Commit`` and ``Abort`` take raw bytes, not structs
-------------------------------------------------------

Symmetric with ``AdbcConnectionReadPartition``, which takes the raw
bytes from a ``partitions[i]`` entry rather than the
``AdbcPartitions`` struct.  Receipts that traveled across processes
arrive as raw bytes; forcing the caller to wrap them in
``AdbcIngestReceipt`` structs (with bogus ``release`` callbacks)
would be friction without benefit.

6. Lost receipts are handled by handle-scoped sweep, not by receipts
--------------------------------------------------------------------

If a worker writes data but its receipt is lost in transit, the
coordinator's receipt list is incomplete.  ``Commit`` will not
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

7. Coordinator may die without calling ``Commit`` or ``Abort``
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

Open questions
==============

These are intentionally unresolved in the initial revision.

- **Single-coordinator commit only.**  Two coordinators calling
  ``Commit`` on the same handle concurrently is undefined.  Should
  drivers be required to detect and reject this, or is it the
  caller's responsibility?
- **Subset writes.**  Today the prototype assumes each worker writes
  the same column set.  Receipts encode the column list, so it is
  possible to support per-worker subsets in the future, but this is
  not specified yet.
- **Append-mode schema validation.**  The schema parameter is
  optional in ``append`` mode.  Should drivers be *required* to
  validate when a schema is supplied?  Currently "should".
- **Streaming Commit.**  Today ``Commit`` takes all receipts at
  once.  For very-many-partition jobs (10k+ workers) it may be
  preferable to incrementally accumulate receipts.  Out of scope for
  v1.

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
