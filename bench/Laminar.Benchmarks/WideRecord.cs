using Laminar.Core;
using Laminar.Ingestion.Layout;

namespace Laminar.Benchmarks;

/// <summary>
/// A flat record with eight fields (40 bytes), like one event with a timestamp and seven
/// parameters. Wide enough that reading one field from whole rows wastes most of each cache line.
/// </summary>
public readonly record struct WideRecord(long Time, float A, float B, float C, float D, float E, float F, float G);

internal static class WideRecords
{
    public static WideRecord Make(long i) =>
        new(i, i * 0.5f, i * 0.25f, i * 0.125f, i, -i, i * 2f, i * 3f);

    // Batches built once and reused by every benchmark invocation. Stores copy records out and
    // never dispose the batch, so the same batches can be appended again and again.
    public static Batch<WideRecord>[] MakeBatches(int batches, int batchSize)
    {
        var result = new Batch<WideRecord>[batches];
        long next = 0;

        for (int b = 0; b < batches; b++)
        {
            var batch = new Batch<WideRecord>(batchSize, b);
            Span<WideRecord> span = batch.Buffer;
            for (int i = 0; i < batchSize; i++)
            {
                span[i] = Make(next++);
            }

            batch.SetCount(batchSize);
            result[b] = batch;
        }

        return result;
    }

    public static void Dispose(Batch<WideRecord>[] batches)
    {
        foreach (Batch<WideRecord> batch in batches)
        {
            batch.Dispose();
        }
    }

    public static RecordLayout<WideRecord> AllColumns() => RecordLayout.Columns<WideRecord>("records", t => t
        .Field("time", r => r.Time)
        .Field("a", r => r.A)
        .Field("b", r => r.B)
        .Field("c", r => r.C)
        .Field("d", r => r.D)
        .Field("e", r => r.E)
        .Field("f", r => r.F)
        .Field("g", r => r.G));
}
