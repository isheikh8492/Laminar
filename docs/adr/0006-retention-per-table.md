# 0006. Retention: how much of each table stays in memory

**Status:** Accepted

Extends ADR 0002 (eviction), ADR 0004 (tables) and ADR 0005 (prepare before start).

## Context

Some data is far too big to keep in memory for a whole run. Measured with the spectral simulator
(7 lasers, 60 channels × 9 features, a waveform per channel), at 90k events/s:

| Per event | Bytes | Data rate | 64 GB of RAM fills in |
|---|---|---|---|
| Features only | 2.1 KB | 195 MB/s | about 5.5 min |
| Features + 60 × 64 samples | 9.6 KB | 890 MB/s | about 70 s |
| Features + 60 × 2,500 samples (oscilloscope) | 295 KB | 27 GB/s | about 2.4 s |

At 2,500 samples, an hour is 97 TB. Even a single memory copy of it at full rate (21.8 GB/s
measured on a desktop, one thread) is more than one machine's memory bus comfortably carries.

But not all data needs keeping. Waveforms are mostly for viewing: the latest few pulses on an
oscilloscope, or the pulses of the events currently on a plot. Features are the experiment and
are recorded in full, while the screen only needs a recent window. An earlier project kept
exactly this split: a ring of the latest 200k events per signal, and an oscilloscope buffer of
the latest 3 captures.

ADR 0002 already frees memory once the recorder has saved it. What is missing is a way to say,
per kind of data, how much stays readable in memory, and to change it while the run continues,
as a user widens or narrows a view.

## Decision

1. **Every table has a retention, which says how much of it stays in memory.** It applies to any
   table, the root records as well as nested ones:

   | Retention | Keeps in memory |
   |---|---|
   | `KeepAll` (the default) | Everything. Memory grows for the whole run. |
   | `Latest(n, max)` | At least the latest `n` items. `n` can change during the run, up to `max`. |
   | `SameAsParent` | Nested tables only: the items of every parent item still in memory. |

   For a nested table, `n` counts **parent items**, not nested ones: `Latest(5)` on waveforms
   means the waveforms of the latest 5 events, however many samples that is. Windows count
   items, not time, so their memory is known in advance.

   ```csharp
   var layout = RecordLayout.Columns<SpectralEvent>("events", t => t
       .Field("time", e => e.Time)
       // … 540 feature fields …
       .Many("waveforms", e => e.Waveforms, TableStorage.Rows, retain: Retention.Latest(5, max: 1_000)),
       retain: Retention.Latest(1_000_000, max: 5_000_000));
   ```

2. **Retention is per table, not per column.** Every column of a table covers the same range of
   items, so one number per table tells a reader what is still there. If columns forgot at their
   own pace, a plot of one feature against another could find an event in one column that the
   other has already dropped. Data that needs a different retention goes in its own table.

3. **A nested table never keeps more than its parent.** A waveform whose event is gone has nothing
   to belong to. `Latest(n)` on a nested table must be no larger than its parent's window; this is
   checked when the layout is built, and when either window changes, the child's is clamped to
   the parent's.

4. **Retention and recording are separate.** Recording decides what goes to disk (ADR 0002's
   durable consumers). Retention decides what stays in memory. A chunk is freed only when both
   are true:
   - it is outside its table's retention window, and
   - every durable consumer recording that table has confirmed it.

   So the features can be recorded in full while only the latest million stay in memory, and
   waveforms can be kept for viewing without being recorded at all. Changing a recorded table's
   retention never loses recorded data; it only changes what stays readable. If the recorder
   falls behind, chunks outside the window wait for it, and memory grows by its lag (ADR 0002's
   recorder lag), not by the run's length.

5. **Indices stay absolute.** Freeing old data never renumbers anything: event 1,000,000 is
   event 1,000,000 for the whole run. Each table publishes two numbers:
   - `Count`: how many items it has ever had, as today;
   - `FirstRetained`: the oldest item still in memory.

   Reading below `FirstRetained` reports the item as evicted, never another item's data.
   Offsets also stay absolute, so asking for an evicted event's waveform reports it evicted.

6. **Windows are exactly n readable items; memory may hold up to one chunk more.** Memory is
   freed one whole chunk at a time, so the oldest chunk may still hold items below the window.
   They are not readable: `FirstRetained` is the window's exact start, the same in every column of
   the table.

7. **What can't survive is never stored.** When a batch alone holds more than a window, as with
   `Latest(5)` waveforms and 900-event batches, only the items that can survive are written. The
   others are counted as evicted on arrival, and their indices and offsets still advance, so
   absolute numbering stays correct. A small viewing window then costs the store almost nothing,
   whatever the incoming rate.

8. **Retention can change during a run.**
   - Any thread may ask for a new `n`. The request is a value the worker picks up between
     batches; the worker applies it, so the store keeps a single writer and the hot path takes no
     locks.
   - **Shrinking** takes effect at once: chunks no longer needed are freed (once recorded, if the
     table is recorded) and `FirstRetained` moves forward. Nothing is copied.
   - **Growing** takes effect for new data: what was already freed is gone, and the window fills
     up as items arrive.
   - `Prepare()` reserves enough for `max`. Growing within it allocates nothing. Asking for more
     than `max` is allowed but allocates, and is counted as allocation after prepare (ADR 0005).
   - Switching between `KeepAll` and `Latest` is allowed; `KeepAll` behaves as an unlimited
     `max`.

9. **Freed chunks are reused only when no reader can still see them.** Chunks freed from a window
   go back to a pool for the same table, so a long run allocates nothing once its windows are
   full. Reuse must never change data a reader is still reading. That was the weakness of the
   earlier project's ring buffer, whose snapshots pointed into memory the writer kept
   overwriting. Until a mechanism for this is in place (for example reader epochs or reference
   counts, chosen when it is built), freed chunks are left to the garbage collector instead of
   reused. That is safe, because a reader holding a chunk keeps it alive, but it allocates, and
   the allocations are counted.

10. **Every eviction is counted.** Per table, metrics report items in memory, `FirstRetained`,
    items evicted (freed from memory, and lost if the table isn't recorded) and items evicted on
    arrival. Recorder lag stays a separate metric.

## Alternatives considered

- **A ring buffer per table that overwrites in place** (the earlier project's design). Simple,
  but readers can see data change under them, a wrapped ring isn't contiguous (which gets in the
  way of SIMD scans), and resizing copies every column under a lock, which pauses ingestion.
  Chunks give the same fixed memory without any of those.
- **Windows by time** ("the last 2 seconds"). Natural for an oscilloscope, but memory then depends
  on the incoming rate and can't be reserved in advance. Time windows can be built on top of item
  windows, by a reader that finds the first item inside the time range.
- **Retention per column.** More flexible, but columns of one table would cover different ranges,
  and every reader would have to check each column separately.
- **Retention fixed for the whole run.** Simpler, but a user widening a plot or opening an
  oscilloscope mid-run is a normal thing to do, and shouldn't need a new pipeline.
- **One window for the whole store.** Can't express "record every feature, keep 5 waveforms",
  which is the case that motivated this.

## Consequences

- `ChunkedColumn` gains a moving start: it frees chunks from the front, keeps absolute indices,
  and can skip ahead without writing (decision 7).
- Stores need a way for the worker to pick up retention requests between batches, and
  `Prepare()` reserves each table's `max`.
- Readers must expect evicted items: `FirstRetained` joins `Count` as part of reading a table,
  and lookups can report "evicted".
- The default stays `KeepAll`, so nothing changes for existing users, but a long run with the
  default still fills memory. The test bench should show the window and evictions per table.
- Reusing chunks safely needs a reader-tracking mechanism, decided when it is built (decision 9).
- Deferred: **buffer swapping**, where the store takes a batch's waveform buffer as a chunk
  instead of copying it and hands an empty chunk back. It removes a full copy of the largest data
  and fits this design's chunk pool, but is only worth building once a measurement shows the copy
  matters.
