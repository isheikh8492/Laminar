// Laminar test bench: runs a source through a pipeline and prints live metrics every second.
//
//   dotnet run --project bench/Laminar.TestBench -- --source sim --rate 100000
//
// Options:
//   --source sim            Which source to run (only "sim" for now).
//   --rate <n>              Records per second.                     Default 100000.
//   --batch <n>             Records per batch.                      Default: about 100 batches/s.
//   --capacity <n>          Buffer size in batches.                 Default 64.
//   --policy <name>         Wait, DropIncoming or DropOldest.       Default Wait.
//   --seconds <n>           Stop after n seconds.                   Default: run until Ctrl+C.

using System.Globalization;
using Laminar.Core;
using Laminar.Ingestion;
using Laminar.Sources.Simulation;

string sourceName = "sim";
double rate = 100_000;
int? batchSize = null;
int capacity = 64;
FullBufferPolicy policy = FullBufferPolicy.Wait;
double seconds = 0;

try
{
    for (int i = 0; i < args.Length; i++)
    {
        string NextValue() => i + 1 < args.Length ? args[++i] : throw new FormatException($"{args[i]} needs a value.");

        switch (args[i])
        {
            case "--source": sourceName = NextValue(); break;
            case "--rate": rate = double.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--batch": batchSize = int.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--capacity": capacity = int.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            case "--policy": policy = Enum.Parse<FullBufferPolicy>(NextValue(), ignoreCase: true); break;
            case "--seconds": seconds = double.Parse(NextValue(), CultureInfo.InvariantCulture); break;
            default: throw new FormatException($"Unknown option {args[i]}.");
        }
    }

    if (sourceName != "sim")
    {
        throw new FormatException($"Unknown source '{sourceName}'. Only 'sim' exists so far.");
    }
}
catch (Exception ex) when (ex is FormatException or ArgumentException or OverflowException)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine("Usage: --source sim [--rate n] [--batch n] [--capacity n] [--policy Wait|DropIncoming|DropOldest] [--seconds n]");
    return 2;
}

// Aim for about 100 batches per second unless told otherwise.
int batch = batchSize ?? (int)Math.Clamp(rate / 100, 1, 10_000);

var source = new SimulatedSource<SimulatedSample>(rate, batch, SimulatedSample.SineWave);
var store = new RowStore<SimulatedSample>();

Console.WriteLine($"source=sim rate={rate:N0}/s batch={batch:N0} capacity={capacity} policy={policy}");
Console.WriteLine("Ctrl+C to stop.");
Console.WriteLine();
Console.WriteLine($"{"time",6} {"appended/s",12} {"fill",6} {"buffered",9} {"in flight",9} {"dropped",12} {"stored",14}");

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

await using var pipeline = new Pipeline<SimulatedSample>(source, store, capacity, policy);
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

        Console.WriteLine(
            $"{tick,5}s {perSecond,12:N0} {m.BufferFill,6:P0} {m.BatchesBuffered,9:N0} {m.BatchesInFlight,9:N0} {m.RecordsDropped,12:N0} {store.Count,14:N0}");
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
Console.WriteLine();
Console.WriteLine($"buffer empty, nothing in flight:  {(settled ? "OK" : "FAIL")}");
Console.WriteLine($"written == appended + dropped:    {(balanced ? "OK" : "FAIL")}");
Console.WriteLine($"store count == appended:          {(stored ? "OK" : "FAIL")}");

return settled && balanced && stored ? 0 : 1;
