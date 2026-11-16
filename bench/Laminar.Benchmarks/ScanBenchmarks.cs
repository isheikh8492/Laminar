using System.Numerics;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Laminar.Core;
using Laminar.Ingestion;
using Laminar.Ingestion.Layout;

namespace Laminar.Benchmarks;

/// <summary>
/// Reading stored data back: summing one field over 10M records (analysis), and reading whole
/// records at random positions (per-record work). Times are per record.
/// </summary>
/// <remarks>
/// Expected: summing one field is much faster from a column, because it reads 4 bytes per record
/// instead of the whole 40-byte row, and contiguous floats let SIMD add several at once. Reading
/// whole records is faster from rows: one place in memory instead of eight.
/// <para>
/// Grouped by category, each with its own baseline, so the Ratio column compares like with like.
/// </para>
/// </remarks>
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class ScanBenchmarks
{
    private const int Records = 10_000_000;
    private const int RandomReads = 100_000;

    private RowStore<WideRecord> _rows = null!;
    private LayoutStore<WideRecord> _columns = null!;
    private ChunkedColumn<float> _columnB = null!;
    private long[] _randomIndices = [];

    [GlobalSetup]
    public void Setup()
    {
        Batch<WideRecord>[] batches = WideRecords.MakeBatches(Records / 10_000, 10_000);
        _rows = new RowStore<WideRecord>();
        _columns = new LayoutStore<WideRecord>(WideRecords.AllColumns());

        foreach (Batch<WideRecord> batch in batches)
        {
            _rows.Append(batch);
            _columns.Append(batch);
        }

        WideRecords.Dispose(batches);
        _columnB = _columns.GetColumn<float>("records", "b");

        // Fixed seed, so every run reads the same positions.
        var random = new Random(42);
        _randomIndices = new long[RandomReads];
        for (int i = 0; i < RandomReads; i++)
        {
            _randomIndices[i] = random.NextInt64(Records);
        }
    }

    // ---- Analysis: sum one field across every record. ----

    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    [BenchmarkCategory("SumOneField")]
    public float SumFieldFromRows()
    {
        float sum = 0;
        ChunkedColumn<WideRecord> records = _rows.Records;
        for (int c = 0; c < records.ChunkCount; c++)
        {
            foreach (WideRecord r in records.GetChunk(c))
            {
                sum += r.B;
            }
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Records)]
    [BenchmarkCategory("SumOneField")]
    public float SumFieldFromColumn()
    {
        float sum = 0;
        for (int c = 0; c < _columnB.ChunkCount; c++)
        {
            foreach (float value in _columnB.GetChunk(c))
            {
                sum += value;
            }
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Records)]
    [BenchmarkCategory("SumOneField")]
    public float SumFieldFromColumnSimd()
    {
        float total = 0;
        for (int c = 0; c < _columnB.ChunkCount; c++)
        {
            ReadOnlySpan<float> chunk = _columnB.GetChunk(c);

            // View the floats as SIMD vectors (8 floats each with AVX2) and add a whole vector per
            // step. The few floats left over at the end are added one by one.
            ReadOnlySpan<Vector<float>> vectors = MemoryMarshal.Cast<float, Vector<float>>(chunk);
            Vector<float> sums = Vector<float>.Zero;
            foreach (Vector<float> v in vectors)
            {
                sums += v;
            }

            total += Vector.Sum(sums);
            for (int i = vectors.Length * Vector<float>.Count; i < chunk.Length; i++)
            {
                total += chunk[i];
            }
        }

        return total;
    }

    // ---- Per-record work: read whole records at random positions. ----

    [Benchmark(Baseline = true, OperationsPerInvoke = RandomReads)]
    [BenchmarkCategory("ReadWholeRecord")]
    public float ReadRecordFromRows()
    {
        float sum = 0;
        foreach (long i in _randomIndices)
        {
            WideRecord r = _rows[i];
            sum += r.Time + r.A + r.B + r.C + r.D + r.E + r.F + r.G;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = RandomReads)]
    [BenchmarkCategory("ReadWholeRecord")]
    public float ReadRecordFromColumns()
    {
        ChunkedColumn<long> time = _columns.GetColumn<long>("records", "time");
        ChunkedColumn<float> a = _columns.GetColumn<float>("records", "a");
        ChunkedColumn<float> b = _columnB;
        ChunkedColumn<float> c = _columns.GetColumn<float>("records", "c");
        ChunkedColumn<float> d = _columns.GetColumn<float>("records", "d");
        ChunkedColumn<float> e = _columns.GetColumn<float>("records", "e");
        ChunkedColumn<float> f = _columns.GetColumn<float>("records", "f");
        ChunkedColumn<float> g = _columns.GetColumn<float>("records", "g");

        float sum = 0;
        foreach (long i in _randomIndices)
        {
            sum += time[i] + a[i] + b[i] + c[i] + d[i] + e[i] + f[i] + g[i];
        }

        return sum;
    }
}
