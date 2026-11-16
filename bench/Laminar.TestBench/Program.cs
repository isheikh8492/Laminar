// Laminar test bench: runs a source through a pipeline and prints live metrics every second.
//
//   dotnet run --project bench/Laminar.TestBench -- --source sim --rate 100000
//   dotnet run --project bench/Laminar.TestBench -- --source events --rate 100000 --detectors 12
//
// Options:
//   --source sim|events     sim: flat sine-wave samples. events: instrument-like events with
//                           per-detector values and a variable-length waveform.  Default sim.
//   --store row|layout      Where records go. events always uses layout.        Default row.
//   --rate <n>              Records per second.                                  Default 100000.
//   --batch <n>             Records per batch.                     Default: about 100 batches/s.
//   --capacity <n>          Buffer size in batches.                              Default 64.
//   --policy <name>         Wait, DropIncoming or DropOldest.                    Default Wait.
//   --seconds <n>           Stop after n seconds.                  Default: run until Ctrl+C.
//   --detectors <n>         events only: values per event.                       Default 8.
//   --samples <min>-<max>   events only: waveform length range.                  Default 32-128.

using System.Globalization;
using Laminar.Core;
using Laminar.Ingestion;
using Laminar.Ingestion.Layout;
using Laminar.Sources.Simulation;

string sourceName = "sim";
string storeName = "row";
double rate = 100_000;
int? batchSize = null;
int capacity = 64;
FullBufferPolicy policy = FullBufferPolicy.Wait;
double seconds = 0;
int detectors = 8;
int minSamples = 32;
int maxSamples = 128;

try
{
    for (int i = 0; i < args.Length; i++)
    {
        string NextValue() => i + 1 < args.Length ? args[++i] : throw new FormatException($"{args[i]} needs a value.");

        switch (args[i])
        {
            case "--source": sourceName = NextValue(); break;
            case "--store": storeName = NextValue(); break;
            case "--rate": rate = double.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--batch": batchSize = int.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--capacity": capacity = int.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--policy": policy = Enum.Parse<FullBufferPolicy>(NextValue(), ignoreCase: true); break;
            case "--seconds": seconds = double.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--detectors": detectors = int.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--samples":
                string[] range = NextValue().Split('-');
                minSamples = int.Parse(range[0], CultureInfo.InvariantCulture);
                maxSamples = int.Parse(range[^1], CultureInfo.InvariantCulture);
                break;
            default: throw new FormatException($"Unknown option {args[i]}.");
        }
    }

    if (sourceName is not ("sim" or "events"))
    {
        throw new FormatException($"Unknown source '{sourceName}'. Use sim or events.");
    }

    if (storeName is not ("row" or "layout"))
    {
        throw new FormatException($"Unknown store '{storeName}'. Use row or layout.");
    }
}
catch (Exception ex) when (ex is FormatException or ArgumentException or OverflowException or IndexOutOfRangeException)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine(
        "Usage: --source sim|events [--store row|layout] [--rate n] [--batch n] [--capacity n] " +
        "[--policy Wait|DropIncoming|DropOldest] [--seconds n] [--detectors n] [--samples min-max]");
    return 2;
}

// Aim for about 100 batches per second unless told otherwise.
int batch = batchSize ?? (int)Math.Clamp(rate / 100, 1, 10_000);

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Don't kill the process; stop the pipeline cleanly instead.
    stop.Cancel();
};

if (seconds > 0)
{
    stop.CancelAfter(TimeSpan.FromSeconds(seconds));
}

if (sourceName == "sim")
{
    var source = new SimulatedSource<SimulatedSample>(rate, batch, SimulatedSample.SineWave);

    if (storeName == "row")
    {
        var rows = new RowStore<SimulatedSample>();
        return await RunAsync("sim → row store", source, rows, () => []);
    }

    var layout = RecordLayout.Columns<SimulatedSample>("samples", t => t
        .Field("index", s => s.Index)
        .Field("value", s => s.Value));
    var store = new LayoutStore<SimulatedSample>(layout);
    return await RunAsync("sim → layout store (columns)", source, store, () => DescribeTables(store));
}
else
{
    var generator = new SimulatedEventGenerator(detectors, minSamples, maxSamples, rate);
    var source = new SimulatedSource<SimulatedEvent>(
        rate, batch, generator.Generate, generator.PayloadBytesPerBatch(batch));

    // Scalars and per-detector values as columns, each waveform as rows.
    var layout = RecordLayout.Columns<SimulatedEvent>("events", t =>
    {
        t.Field("time", e => e.Time).Field("fsc", e => e.Fsc).Field("ssc", e => e.Ssc);
        for (int d = 0; d < detectors; d++)
        {
            int k = d;
            t.Field($"FL{d + 1}", (e, payload) => payload.Get(e.Parameters)[k]);
        }

        t.Many("waveform", e => e.Waveform, TableStorage.Rows);
    });

    var store = new LayoutStore<SimulatedEvent>(layout);
    return await RunAsync(
        $"events ({detectors} detectors, {minSamples}-{maxSamples} samples) → layout store",
        source, store, () => DescribeTables(store));
}

// Runs one pipeline until stopped, printing metrics every second, then checks the accounting.
// Generic, so the same loop works for any record type and store.
async Task<int> RunAsync<T>(string title, ISource<T> source, IRecordStore<T> store, Func<IEnumerable<string>> describe)
{
    Console.WriteLine($"{title}");
    Console.WriteLine($"rate={rate:N0}/s batch={batch:N0} capacity={capacity} policy={policy}");
    Console.WriteLine("Ctrl+C to stop.");
    Console.WriteLine();
    Console.WriteLine(
        $"{"time",6} {"appended/s",12} {"fill",6} {"buffered",9} {"in flight",9} {"dropped",12} {"stored",14}" +
        $" │ {"gen0",5} {"gen1",5} {"gen2",5} {"alloc MB/s",11} {"gc pause",9}");

    await using var pipeline = new Pipeline<T>(source, store, capacity, policy);

    // GC readings at the start, so the per-second numbers cover only this run.
    GcReading startGc = GcReading.Now();
    GcReading lastGc = startGc;

    pipeline.Start();

    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    long lastAppended = 0;
    int tick = 0;

    try
    {
        while (await timer.WaitForNextTickAsync(stop.Token) && !pipeline.Completion.IsCompleted)
        {
            tick++;
            PipelineMetrics m = pipeline.GetMetrics();
            long perSecond = m.RecordsAppended - lastAppended;
            lastAppended = m.RecordsAppended;

            // What the garbage collector did during this second: collections of each
            // generation, megabytes allocated, and how long the app was paused for collections.
            GcReading gc = GcReading.Now();
            GcReading d = gc - lastGc;
            lastGc = gc;

            Console.WriteLine(
                $"{tick,5}s {perSecond,12:N0} {m.BufferFill,6:P0} {m.BatchesBuffered,9:N0} {m.BatchesInFlight,9:N0} {m.RecordsDropped,12:N0} {store.Count,14:N0}" +
                $" │ {d.Gen0,5} {d.Gen1,5} {d.Gen2,5} {d.AllocatedBytes / 1e6,11:N1} {d.Pause.TotalMilliseconds,7:N1}ms");
        }
    }
    catch (OperationCanceledException)
    {
        // Ctrl+C or --seconds elapsed.
    }

    Console.WriteLine();
    Console.WriteLine("Stopping...");

    try
    {
        await pipeline.StopAsync();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Pipeline failed: {ex.Message}");
        return 1;
    }

    PipelineMetrics final = pipeline.GetMetrics();
    bool settled = final.BatchesBuffered == 0 && final.BatchesInFlight == 0;
    bool balanced = final.RecordsWritten == final.RecordsAppended + final.RecordsDropped;
    bool stored = store.Count == final.RecordsAppended;

    Console.WriteLine($"written  {final.RecordsWritten,14:N0} records in {final.BatchesWritten:N0} batches");
    Console.WriteLine($"appended {final.RecordsAppended,14:N0}");
    Console.WriteLine($"dropped  {final.RecordsDropped,14:N0}");
    Console.WriteLine($"stored   {store.Count,14:N0}");

    foreach (string line in describe())
    {
        Console.WriteLine(line);
    }

    GcReading total = GcReading.Now() - startGc;
    Console.WriteLine(
        $"gc       {total.Gen0} gen0, {total.Gen1} gen1, {total.Gen2} gen2 collections; " +
        $"{total.AllocatedBytes / 1e6:N0} MB allocated; {total.Pause.TotalMilliseconds:N1} ms paused");

    Console.WriteLine();
    Console.WriteLine($"buffer empty, nothing in flight:  {(settled ? "OK" : "FAIL")}");
    Console.WriteLine($"written == appended + dropped:    {(balanced ? "OK" : "FAIL")}");
    Console.WriteLine($"store count == appended:          {(stored ? "OK" : "FAIL")}");

    return settled && balanced && stored ? 0 : 1;
}

// One line per table: its storage and how many items it holds.
static IEnumerable<string> DescribeTables<T>(LayoutStore<T> store)
{
    foreach (TableSchema table in store.Schema.Tables)
    {
        // The root table holds one item per record. A nested table's last offset is where its
        // final parent's items end, which is its total item count.
        long items = table.Parent is null ? store.Count : LastOffset(store.GetOffsets(table.Name));

        string under = table.Parent is null ? "" : $" under {table.Parent}";
        string what = table.Storage == TableStorage.Rows
            ? $"rows of {table.ItemType.Name}"
            : $"{table.Fields.Count} columns";

        yield return $"  table {table.Name,-10} {what,-18}{under,-14} {items,14:N0} items";
    }

    static long LastOffset(ChunkedColumn<long> offsets) => offsets[offsets.Count - 1];
}

// A snapshot of the garbage collector's running totals. Subtracting two gives what happened
// between them. Collection counts include higher generations: a gen2 collection also counts as
// gen1 and gen0, as .NET reports them. (Types must come after all top-level code in this file.)
internal readonly record struct GcReading(int Gen0, int Gen1, int Gen2, long AllocatedBytes, TimeSpan Pause)
{
    public static GcReading Now() => new(
        GC.CollectionCount(0),
        GC.CollectionCount(1),
        GC.CollectionCount(2),
        GC.GetTotalAllocatedBytes(precise: false),
        GC.GetTotalPauseDuration());

    public static GcReading operator -(GcReading a, GcReading b) => new(
        a.Gen0 - b.Gen0,
        a.Gen1 - b.Gen1,
        a.Gen2 - b.Gen2,
        a.AllocatedBytes - b.AllocatedBytes,
        a.Pause - b.Pause);
}
