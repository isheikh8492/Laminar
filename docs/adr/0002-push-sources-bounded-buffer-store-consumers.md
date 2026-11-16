# 0002. Push-based sources, bounded buffer, store as the source of truth

**Status:** Accepted. Extended by ADR 0006: a chunk is freed once recorded **and** outside its table's retention window.

## Context

High-rate sources deliver data when they have it: a USB callback fires, a socket receives, a
DMA buffer fills, a market feed sends a tick. A source cannot wait to be asked, and hardware or a
remote sender cannot be told to slow down. Data also arrives in bursts, briefly faster than the
next stage can handle.

Many parts of a system want the same data: a recorder writing to disk, plots, region filters,
statistics, and possibly external dataflow tools such as Bonsai.

A long run does not fit in memory. At 1M records/s × 20 fields × 4 bytes, the data grows by
80 MB/s, nearly 5 GB per minute. Old data must eventually leave memory.

Design rule from AGENTS.md: recording never loses data; display may drop it. Every drop is
counted, never silent.

## Decision

The data flows through four stages:

```
source ──push──▶ [bounded buffer] ──▶ ingestion ──▶ store ──push──▶ consumers
```

Source, buffer and ingestion are generic over the record type `T`; only the store knows the
data's shape (ADR 0001).

1. **Sources push.** A source hands over batches whenever it has them. Writing a source stays
   simple: when data arrives, push it.
2. **A bounded buffer sits between sources and ingestion.** It has a fixed size and absorbs
   bursts. When it is full, an explicit policy decides what happens (for example, drop the
   newest batch), and every drop is counted.
3. **One ingestion worker** takes batches from the buffer, validates them, and appends them to
   the store, whichever store the user chose (ADR 0001). This is the only way data enters the
   store.
4. **The store is the source of truth** and pushes new data to consumers. There are two kinds:

   | | Durable consumer (the recorder) | Live consumer (plots, filters, statistics, external tools) |
   |---|---|---|
   | May skip data? | Never | Yes; it may jump to the latest data |
   | Holds back eviction? | Yes, until it confirms a chunk is written | No |
   | If it falls behind | The store fills; the buffer's full policy applies, with counted drops | It skips ahead and counts what it skipped |

   The store evicts a chunk from memory only after every durable consumer has confirmed it.

## Alternatives considered

- **Push all the way through, with no buffer (Rx style).** Simple to compose, but there is no
  backpressure: during a burst, data piles up in memory without limit, or is lost in a driver or
  OS buffer where nobody counts it. Silent loss breaks the core design rule.
- **Pull-based sources (ingestion asks the source for data).** Does not match how hardware
  works. The source would need its own hidden buffer anyway, and losses there would be
  invisible.
- **The recorder on the ingestion path, before the store.** Gives two separate ways for data to
  flow, so what is on disk and what is on screen could disagree. With the recorder as a store
  consumer, there is one way in, and every consumer sees the same data.

## Consequences

- Sources are easy to write, and any push-based stream can become a source with a small adapter,
  including an `IObservable<T>` from Bonsai.
- The store's output stream can feed external dataflow tools.
- Recorders are pluggable: different file formats or a network forwarder are just durable
  consumers.
- The store must track each durable consumer's position, and eviction waits for the slowest one.
- New health metrics: buffer fill level, drops at the buffer, skips by live consumers, and
  **recorder lag** (data in the store but not yet on disk). Recorder lag is exactly what a crash
  would lose, so it must stay small and visible.
- Data has not been persisted until the recorder confirms it. Crash safety depends on keeping
  recorder lag low.
