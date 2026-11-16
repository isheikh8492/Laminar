# 0001. Struct-defined records and pluggable storage

**Status:** Accepted. Partly superseded by ADR 0003: the `unmanaged` constraint applies to the
stores and zero-copy decoding, not the whole pipeline. Extended by ADR 0004: storage is chosen
per part of a record through a layout, and nested data is stored with offsets.

## Context

Laminar is a high-rate ingestion library for .NET. Its users ingest many kinds of data:
instrument events, multi-channel signals, sensor readings, telemetry, market ticks. What they
share is data arriving fast and in bursts, made of **fixed-size records**: each record has the
same fields every time.

They want different things done with the data once it arrives:

| Use | What it reads | Best layout |
|---|---|---|
| Plots, histograms, region filters, statistics | A few fields across many records, constantly | Columns |
| Per-record processing (decode, classify, forward) | Every field of each record, in order | Rows |
| Forwarding to a file, database or network | Whole batches, once | Whatever the destination needs |

No single storage layout suits all of them.

## Decision

1. **The pipeline is generic over the record type.** Source, buffer and ingestion move batches
   of any `T` and know nothing about its fields (see ADR 0002). Only the destination knows the
   data's shape.
2. **The schema is the user's own `unmanaged` struct.** The struct's fields are the record's
   fields, and each keeps its own type. Raw bytes can be viewed as structs without copying
   (`MemoryMarshal.Cast`). A runtime schema descriptor, for formats known only at runtime (for
   example from a config file), may be added later as a second way in.
3. **Storage is pluggable.** A destination has a minimal contract: append a batch, and report how
   many records it holds. There is deliberately no shared read API, because rows are read by
   index and columns by field.
4. **The library ships two stores:**
   - **Row store:** append-only chunks of `T[]`. Simple, and the natural fit for per-record work.
   - **Column store:** one array per struct field, in the field's own type, appended in chunks.
     Fields are discovered once per type.
5. **Analytics are built on the column store only.** Plots, histograms, region filters and
   statistics target one layout and are written once.

## Alternatives considered

- **One columnar store for everyone.** Best for analytics, but it forces columns on users who
  only process or forward records, and makes them pay a transpose they don't need.
- **One store with a layout setting.** Every consumer built on it would have to support both
  layouts, doubling code, tests and benchmarks.
- **A shared read API across stores.** Rows and columns are read differently; a common
  interface would make both worse to use.
- **A runtime byte descriptor as the main schema.** Needed for formats known only at runtime, but
  for most C# users a struct is the obvious, type-checked way to describe a record. It can be
  added later without changing the pipeline.
- **Converting every field to `float`.** Simpler columns, but `float` represents integers exactly
  only up to 2²⁴ (about 16.7 million), so timestamps, counters and IDs would lose precision.

## Consequences

- Users who only need ingestion can pick any store or write their own destination.
- The row store is simple, so it is built first; the pipeline can be finished and tested before
  the harder column store exists.
- Record types must be `unmanaged`: no strings or other references inside a record.
- The column store must handle columns of different element types side by side.
- Analytics depend on the column store; users of the row store don't get them.
- Variable-length data, such as a captured waveform per event, does not fit a fixed-size record.
  It will need a list column (an offsets array plus one values array, as in Apache Arrow), added
  later as a new column type without changing the pipeline.
