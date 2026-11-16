using BenchmarkDotNet.Attributes;
using Laminar.Core;
using Laminar.Ingestion;
using Laminar.Ingestion.Layout;
using Laminar.Sources.Simulation;

namespace Laminar.Benchmarks;

/// <summary>
/// How fast each store takes records in: 1M flat records per invocation, in batches of 1,000.
/// Times are per record (OperationsPerInvoke), and include allocating the store's chunks.
/// </summary>
/// <remarks>
/// Expected: whole rows are one bulk copy per chunk, so the row store and a Rows layout should be
/// fastest. Columns call one accessor per field per record, so cost grows with the number of
/// fields stored.
/// </remarks>
[MemoryDiagnoser]
public class AppendBenchmarks
{
    private const int BatchSize = 1_000;
    private const int Batches = 1_000;
    private const int Records = BatchSize * Batches;

    private Batch<WideRecord>[] _batches = [];
    private RecordLayout<WideRecord> _rowsLayout = null!;
    private RecordLayout<WideRecord> _allColumns = null!;
    private RecordLayout<WideRecord> _twoColumns = null!;

    [GlobalSetup]
    public void Setup()
    {
        _batches = WideRecords.MakeBatches(Batches, BatchSize);
        _rowsLayout = RecordLayout.Rows<WideRecord>("records");
        _allColumns = WideRecords.AllColumns();
        _twoColumns = RecordLayout.Columns<WideRecord>("records", t => t
            .Field("time", r => r.Time)
            .Field("a", r => r.A));
    }

    [GlobalCleanup]
    public void Cleanup() => WideRecords.Dispose(_batches);

    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    public long RowStore() => AppendAll(new RowStore<WideRecord>());

    [Benchmark(OperationsPerInvoke = Records)]
    public long LayoutRows() => AppendAll(new LayoutStore<WideRecord>(_rowsLayout));

    [Benchmark(OperationsPerInvoke = Records)]
    public long LayoutTwoColumns() => AppendAll(new LayoutStore<WideRecord>(_twoColumns));

    [Benchmark(OperationsPerInvoke = Records)]
    public long LayoutEightColumns() => AppendAll(new LayoutStore<WideRecord>(_allColumns));

    private long AppendAll(IRecordStore<WideRecord> store)
    {
        foreach (Batch<WideRecord> batch in _batches)
        {
            store.Append(batch);
        }

        return store.Count;
    }
}

/// <summary>
/// How fast a layout store takes nested events: scalars and 8 detector values as columns, plus a
/// 32–128 sample waveform per event as rows. 100k events per invocation; times are per event.
/// </summary>
[MemoryDiagnoser]
public class EventAppendBenchmarks
{
    private const int BatchSize = 1_000;
    private const int Batches = 100;
    private const int Events = BatchSize * Batches;
    private const int Detectors = 8;

    private Batch<SimulatedEvent>[] _batches = [];
    private RecordLayout<SimulatedEvent> _layout = null!;

    [GlobalSetup]
    public void Setup()
    {
        var generator = new SimulatedEventGenerator(Detectors, minSamples: 32, maxSamples: 128, eventsPerSecond: 100_000);
        _batches = new Batch<SimulatedEvent>[Batches];
        long next = 0;

        for (int b = 0; b < Batches; b++)
        {
            var batch = new Batch<SimulatedEvent>(BatchSize, b, generator.PayloadBytesPerBatch(BatchSize));
            Span<SimulatedEvent> span = batch.Buffer;
            for (int i = 0; i < BatchSize; i++)
            {
                span[i] = generator.Generate(next++, batch);
            }

            batch.SetCount(BatchSize);
            _batches[b] = batch;
        }

        _layout = RecordLayout.Columns<SimulatedEvent>("events", t =>
        {
            t.Field("time", e => e.Time).Field("fsc", e => e.Fsc).Field("ssc", e => e.Ssc);
            for (int d = 0; d < Detectors; d++)
            {
                int k = d;
                t.Field($"FL{d + 1}", (e, payload) => payload.Get(e.Parameters)[k]);
            }

            t.Many("waveform", e => e.Waveform, TableStorage.Rows);
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (Batch<SimulatedEvent> batch in _batches)
        {
            batch.Dispose();
        }
    }

    [Benchmark(OperationsPerInvoke = Events)]
    public long EventsWithWaveforms()
    {
        var store = new LayoutStore<SimulatedEvent>(_layout);
        foreach (Batch<SimulatedEvent> batch in _batches)
        {
            store.Append(batch);
        }

        return store.Count;
    }
}
