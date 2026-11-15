# Laminar

Real-time ingestion pipeline for instrument data, built as a learning project and as groundwork
for the Arago worksheet. C# / .NET 10.

## Working agreement: mentor only

I'm writing this code myself to learn. AI agents act as tutor and reviewer only.

- **Do not write or edit project code.**
- **Hints first.** When I'm stuck, give a hint or a question before an answer. Give the full answer only if I ask for it.
- **Snippets are illustrative.** Short examples of a concept are fine; don't hand me a finished implementation of the thing I'm building.
- **Review my diffs.** When I ask for a review, read the actual code, run `dotnet build` and `dotnet test`, and point out bugs, threading or allocation problems, and anything that breaks the rules below. Explain *why*.
- **Explain results.** Help me read benchmark output, test failures and live metrics.

## Layout

| Path | Purpose |
|---|---|
| `src/Laminar.Core` | Schema, records, batches, chunks, read-API contracts |
| `src/Laminar.Ingestion` | Pipeline, worker, RecordStore, policies, metrics |
| `src/Laminar.Sources.Simulation` | Synthetic event source with an adjustable rate |
| `tests/*` | xUnit correctness tests |
| `tests/Laminar.Architecture.Tests` | Enforces the dependency rules below |
| `bench/Laminar.Benchmarks` | BenchmarkDotNet microbenchmarks, run on demand |
| `bench/Laminar.TestBench` | Console app: run a source, print live metrics every second |
| `docs/adr/` | One short file per architecture decision |

Shared build settings live in `Directory.Build.props`; all NuGet versions live in `Directory.Packages.props` (central package management, so `.csproj` files have no versions).

## Dependency rules

- Core depends on nothing.
- Ingestion depends only on Core.
- `Sources.*` depend only on Core.
- Nothing in `src/` references WPF or any UI.
- Only `bench/` and `tests/` may reference everything.

## Design rules

- **Recording never loses data; display may drop it.** Every drop is counted, never silent.
- **Bounded queues with an explicit full-queue policy.** Queue fill is the key health metric.
- **Batches, not single events.** Columnar storage (one array per parameter), pooled buffers, no per-event allocations on the hot path.
- **The UI thread only draws.** Ingestion and computation happen on background threads.
- Don't create projects for future sources (Serial, Socket, Parquet) until work on them starts.

## Commands

```
dotnet build
dotnet test
dotnet run --project bench/Laminar.TestBench -- --source sim --rate 100000
dotnet run -c Release --project bench/Laminar.Benchmarks
```
