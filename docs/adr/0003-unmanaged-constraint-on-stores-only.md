# 0003. The `unmanaged` constraint applies to stores, not the pipeline

**Status:** Accepted

Supersedes the part of ADR 0001 that says record types must be `unmanaged` everywhere.

## Context

ADR 0001 made the record type an `unmanaged` struct and listed as a consequence that record types
must be `unmanaged`, with no strings or other references. Taken literally, that applies to the
whole library.

Not all incoming data fits. Some sources produce nested objects: classes with strings, lists and
child objects, such as decoded JSON telemetry. Users with that data still need what the pipeline
provides: bounded buffering, full-buffer policies, counted drops and metrics.

Looking at each part of the library, only some of it depends on `unmanaged`:

| Part | Needs `unmanaged`? | Why |
|---|---|---|
| Batch, bounded buffer, full policies, drop counting, metrics | No | They move and count batches; they never look inside a record |
| Pooled batch buffers | No | `ArrayPool<T>` works for any `T`; references must be cleared on return so the GC can free what they point to |
| Zero-copy decoding from bytes (`MemoryMarshal.Cast`) | Yes | Only valid for types with a fixed, reference-free layout |
| Row store and column store | Yes | They keep records in contiguous memory and split fields into typed columns |

## Decision

1. **`Batch<T>` and the pipeline (sources, bounded buffer, ingestion, policies, metrics) put no
   constraint on `T`.**
2. **The constraint `where T : unmanaged` applies only where it is needed:** the row store, the
   column store, and zero-copy decoding helpers.
3. **Pooled buffers are cleared on return only when `T` holds references**, checked with
   `RuntimeHelpers.IsReferenceOrContainsReferences<T>()`. For `unmanaged` types, returning a
   buffer stays free.
4. Users with nested data choose one of:
   - **Flatten at the source** into one or more `unmanaged` record types, linked by IDs, with
     strings turned into numeric IDs through a lookup table (dictionary encoding). This is the
     recommended path at high rates and gives access to the stores and analytics.
   - **Keep their objects** and use the pipeline with their own destination (for example a
     database writer).

## Alternatives considered

- **Keep `unmanaged` everywhere (ADR 0001 as written).** Simple and fast, but it shuts out
  every user with nested data, even though the buffering and drop accounting they need never look
  inside a record.
- **Make the stores accept any `T` too.** A store of object references loses contiguous memory
  and typed columns, which are the reason the stores exist.

## Consequences

- The pipeline serves more users: anything that can be put in a batch can be buffered, counted
  and measured.
- The compiler enforces the boundary: using the row or column store with a reference type fails
  to compile.
- For reference types, every record is a heap allocation. At high rates the garbage collector
  becomes the bottleneck, and Laminar cannot remove that cost. The documentation should
  recommend flattening for those cases.
- Buffer pooling has two paths (clear or don't clear on return), chosen once per type.
