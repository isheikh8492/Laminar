# 0004. Layouts: row or column storage chosen per part of a nested record

**Status:** Accepted

Extends ADR 0001. Replaces its column store whose "fields are discovered once per type", and its
note that variable-length data would need a list column later.

## Context

Real data is nested, and its parts want different storage:

```
Frame                              (from the wire: header + N events)
├── FrameSeq, BaseTimestamp, Temperature       one value per frame
└── Events[N]
    ├── Timestamp, FSC, SSC, FL1 …              one value per event
    └── Waveform[0…M]                           many values per event, variable length
```

- Scalars that are plotted, histogrammed or filtered want **columns**.
- A waveform or trace that is viewed whole wants **rows**: its samples side by side.
- The parts must stay linked: event 42 and its waveform, event 42 and its frame.
- Some fields are only known at runtime. A cytometry panel chooses its detectors per
  experiment, so its parameters can't be fixed C# fields.
- Nested arrays inside records allocate per event, which breaks the "no per-event allocations"
  rule (AGENTS.md).
- Hardware frames carry their own sequence numbers. A gap means data was lost before Laminar saw
  it, and that loss must be counted too.

ADR 0001 offered one layout per store, chosen for the whole record type. That can't express any
of the above.

## Decision

1. **`T` is the unit of analysis**, usually the event. Sources unpack whatever arrives (frames,
   datagrams, DMA blocks) into batches of `T`. One frame may become one batch, several frames
   one batch, or one frame several batches. **A batch never splits a record**: a record and its
   nested data always travel together.

2. **Nested data travels in a pooled payload buffer on the batch.** Each batch may carry one
   pooled byte buffer. Records hold `PayloadSlice<TItem>` values (a position and a length) instead
   of arrays. Records stay `unmanaged` and fixed-size, and there are no allocations per record.

3. **Stores receive the whole batch.** `IRecordStore<T>.Append` takes the `Batch<T>`, so a store
   can read the records and resolve their payload slices. The store copies what it needs and
   never keeps the batch.

4. **A layout says where each part of `T` goes.** The user describes it in C#, with a builder:
   - **Tables**, each stored as `Rows` (whole items side by side) or `Columns` (one array per
     field), chosen per table.
   - **`Field`** selects one value per record. It can read the record and its payload, so fields
     known only at runtime (for example detector `i` in a parameter slice) are just a loop in
     the builder.
   - **`Many`** selects a variable number of items per record: a nested table, which may itself
     contain more `Many`, to any depth.

5. **Nested tables are stored flattened, with offsets** (the Apache Arrow list layout). All
   children of all parents are stored back to back in one table, and an offsets array says
   where each parent's children start: parent `i` owns children `offsets[i]` to
   `offsets[i + 1]`. Each nesting level has its own offsets. Finding a child's parent is a binary
   search over the offsets.

6. **One append fills every table.** The single ingestion worker appends to all tables of a
   layout in one call, then publishes one count. Record `i` means the same record in every
   table, for every reader.

7. **A layout has two halves:** a **schema** (plain data: table and field names, element types,
   storage, nesting) and **accessors** (code: how to read each value from `T`). The schema is
   shaped like an Arrow schema so it can later be saved in file headers, loaded from JSON, and
   read from other languages, without changing the stores.

8. **`LayoutStore<T>` is just another `IRecordStore<T>`.** The source, buffer, worker and
   pipeline don't change. `RowStore<T>` stays as the simplest store: whole records as rows, with
   no layout.

9. **Frame-level data** is handled one of three ways, all expressible with a layout:
   - **Fold it into each record:** for example, absolute time = frame base time + event delta.
     This is the common case.
   - **Keep a frames table:** make `T` the frame and nest events with `Many`.
   - **Consume it in the source:** checksums and framing bytes go no further than the source.

10. **Sources count loss upstream of Laminar.** A source that sees the hardware's own sequence
    numbers counts the gaps it finds. That loss is reported in the metrics next to the buffer's
    drops, so the accounting covers the whole chain:

    ```
    instrument → [lost in transport] → source → [dropped by policy] → buffer → worker → store
    ```

11. **Out of scope:** a declarative language for wire formats (bytes → `T`). If it is ever
    added, it belongs on the source side, separate from layouts (`T` → tables).

## Alternatives considered

- **A custom layout language (YAML, JSON or a DSL) now.** It needs a parser, validation, error
  messages and versioning, and sooner or later loops, conditions and computed fields: C#
  rebuilt badly. The C# builder has all of that, plus type checking and IntelliSense. The schema
  half can still be loaded from JSON later.
- **One layout for the whole record type (ADR 0001 as written).** It can't store scalars as
  columns and waveforms as rows in the same recording.
- **Arrays inside records.** Simple, but one allocation per record. At 100k records/s the
  garbage collector becomes the bottleneck, and records stop being `unmanaged`.
- **Fixed-size arrays inside records, sized to the longest case.** Wastes memory on every short
  record, and caps the length.
- **A separate pipeline for each table.** Tables would be appended at different moments, so a
  reader could see an event without its waveform.
- **Fields discovered by reflection only.** Can't express runtime fields, computed fields or
  renames. It may come later as a shortcut that fills in the same builder.

## Consequences

- One recording can mix layouts: columns for analysis, rows for per-record views.
- Accessors are delegate calls, once per field per record. That's small next to the rest of
  ingestion; a source generator can remove the cost later without changing the schema.
- `Batch<T>` gains a payload buffer and its own rules. A span handed out for writing is only
  valid until the next payload allocation, because the buffer may grow and move.
- `IRecordStore<T>.Append` changes from a span of records to the batch.
- A `RowStore<T>` of records that hold payload slices stores positions in batches that no longer
  exist. Records with payload need a `LayoutStore<T>`.
- Analytics still target column tables only (ADR 0001, point 5), now as column tables inside a
  layout store.
- The metrics gain source-side loss counters.
- Recordings can become self-describing, because the schema can be written with the data.
