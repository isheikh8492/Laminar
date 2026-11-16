# 0005. One path: unmanaged records, a declared schema, prepare before start

**Status:** Accepted

Supersedes ADR 0003. Records must be `unmanaged` everywhere again, as ADR 0001 first said.

## Context

ADR 0003 let the pipeline carry any `T`, including classes and records holding references, so
users with nested objects could still get buffering and drop counting. Two things since then
argue against keeping that second path.

**The guarantees only hold on the fast path.** Benchmarks and live runs (2M events/s with
waveforms, 7.4 ms of GC pauses in 10 s) show what Laminar is for: no allocations per event, a
quiet garbage collector, and columns that SIMD can scan. Each of those breaks as soon as a record
holds references. With two paths, every promise needs an "unless": no allocations *unless `T` is
a class*, a quiet GC *unless records hold references*, analytics *unless you took the other
path*. A pipeline that carries any `T` with no schema and no warm-up is just
`System.Threading.Channels`, which users can already use directly.

**Push sources can't wait for warm-up.** The source decides when data arrives, and hardware
can't be asked to pause. The first second of a run showed 9 collections, against 0 or 1 a second
afterwards: GC budgets still small, first chunks allocated, the array pool still empty.
Anything done lazily on first use lands at the start of acquisition, which is often a burst.
Warm-up has to be a step that happens before the source may push, and it can only be done
properly if the record shape and sizes are known in advance.

## Decision

1. **One path.** `where T : unmanaged` applies to every part of the pipeline: `Batch<T>`, the
   bounded buffer, the ingestion worker, the pipeline, sources, writers and stores. Nested,
   variable-length data travels in payload slices (ADR 0004). Turning raw bytes or objects into
   `T` stays the source's job: that is where messy input becomes clean records.

2. **The schema is declared, then frozen.** Before a run, the developer fixes:
   - **the record type `T`**, at compile time;
   - **the store and its layout**, at runtime but before preparing: a `RowStore<T>` (the schema
     is `T` itself) or a `LayoutStore<T>` (an explicit layout, for example detectors chosen per
     experiment);
   - **sizing**: records per batch, payload bytes per batch, buffer capacity, and optionally how
     many records to reserve.

   The schema can't change during a run. A different panel is a new pipeline and a new store,
   just as a cytometer starts a new file. Variable lengths are fine; the schema fixes the shape.

3. **Lifecycle: Created → Prepared → Running → Stopped.**
   - `Prepare()` allocates everything the run needs up front: the batch pool and reserved store
     chunks.
   - `Start()` requires a prepared pipeline and throws otherwise.
   - After `Start()`, the only allocation that is expected is store growth beyond what was
     reserved: the recording itself.

4. **The pipeline owns a batch pool.** Sources rent batches from their writer instead of creating
   them. The pool is filled in `Prepare()` with `buffer capacity + 1 (the worker's batch) + one
   per source` batches, each with its record array and payload buffer already allocated.
   Disposing a batch returns it to the pool. This removes every allocation per batch after
   prepare, and puts a known limit on the memory in flight.

5. **Wrong sizing never breaks a run; it is counted.** If the pool runs dry, a new batch is
   allocated and counted (`BatchesAllocatedAfterPrepare`). If a batch's payload outgrows its
   buffer, it grows and is counted (`PayloadGrowths`). Either count above zero means the sizing
   was too small, and the metrics say so. This is the same rule as drops: never silent.

6. **Stores can reserve.** `IRecordStore<T>.Reserve(records)` allocates chunks in advance. A
   layout store also reserves nested tables, with an estimate of items per record.

7. **Pooled arrays are never cleared.** `unmanaged` records hold no references, so returning
   arrays needs no clearing and the per-type check from ADR 0003 goes away.

8. **Threads and compilation.**
   - The worker runs as one long-running loop that allocates nothing while it waits.
   - Code compilation: apps that need a steady first second should compile ahead of time
     (ReadyToRun or Native AOT) or turn tiered compilation off, so no method is recompiled
     during acquisition. Laminar doesn't change process-wide settings itself.
   - A dedicated worker thread, and warming up the store's code inside `Prepare()`, are deferred
     until a measurement shows they are needed.

## Alternatives considered

- **Keep two paths (ADR 0003).** More users could get started without a struct, but every
  guarantee would need small print, every feature would have two modes, and the slow path offers
  little beyond `System.Threading.Channels`.
- **Lazy warm-up on first use.** Simpler API, but the cost lands exactly when a push source starts
  sending, often in a burst.
- **Block the source when the pool is empty.** Bounded memory without exceptions, but a source
  could then wait on the pool as well as on the buffer, and a sizing mistake would look like a
  stall instead of a number. Allocating and counting keeps the buffer's policy as the only place
  that decides about backpressure.
- **Use the shared `ArrayPool` instead of a pipeline pool.** It makes no promise to keep 64 large
  arrays, so some get reallocated later, and its memory isn't limited by the pipeline's sizing.

## Consequences

- Users must define a struct, and a layout if they want columns, before ingesting anything.
  Prototyping with existing objects isn't possible; they flatten at the source.
- Guarantees can be stated without exceptions, and the test bench's GC columns become a check:
  after prepare, allocation should be store growth only.
- Sources change: they rent batches from the writer, and their batch size must fit the pool's.
- The pipeline takes its sizing as options, and `Prepare()` becomes a required call.
- Metrics gain `BatchesAllocatedAfterPrepare` and `PayloadGrowths`.
- Batches can still be created standalone, outside a pipeline, for tools and benchmarks; those
  borrow from the shared array pool as before.
- A pooled batch that is used after being disposed may already belong to another source. That
  can't always be detected; owning a batch correctly matters more than before.
