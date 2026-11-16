using System.Diagnostics;
using Laminar.Core;

namespace Laminar.Sources.Simulation;

/// <summary>
/// A synthetic source that produces records at a steady average rate, for testing and benchmarks.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// Records are made by a function you supply, called with each record's index (0, 1, 2, …), so the
/// same source works for any record type.
/// </para>
/// <para>
/// <b>Pacing:</b> the source keeps track of how many records should exist by now
/// (elapsed time × rate) and pushes full batches whenever it is at least one batch behind. If the
/// pipeline makes it wait, it catches up afterwards in a burst, the way a real instrument's backlog
/// would arrive. Choose a batch size that gives a sensible number of batches per second; at
/// 100 records/s, a batch of 1,000 would only appear every 10 seconds.
/// </para>
/// </remarks>
public sealed class SimulatedSource<T> : ISource<T>
{
    private readonly Func<long, Batch<T>, T> _generate;
    private readonly int _payloadBytesPerBatch;

    /// <summary>
    /// Creates a simulated source for flat records, with no nested data.
    /// </summary>
    /// <param name="recordsPerSecond">The average rate to produce records at.</param>
    /// <param name="batchSize">How many records go in each batch.</param>
    /// <param name="generate">Makes the record with the given index.</param>
    /// <exception cref="ArgumentOutOfRangeException">The rate or the batch size is zero or negative.</exception>
    public SimulatedSource(double recordsPerSecond, int batchSize, Func<long, T> generate)
        : this(recordsPerSecond, batchSize, Wrap(generate))
    {
    }

    /// <summary>
    /// Creates a simulated source for records with nested data (ADR 0004). The generator also
    /// receives the batch being filled, so it can put the record's nested data in the batch's
    /// payload with <see cref="Batch{T}.AllocatePayload"/>.
    /// </summary>
    /// <param name="recordsPerSecond">The average rate to produce records at.</param>
    /// <param name="batchSize">How many records go in each batch.</param>
    /// <param name="generate">Makes the record with the given index, using the given batch's payload.</param>
    /// <param name="payloadBytesPerBatch">
    /// Payload bytes to reserve in each new batch, if roughly known. Avoids the buffer growing
    /// several times per batch; 0 lets it grow as needed.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The rate or the batch size is zero or negative, or <paramref name="payloadBytesPerBatch"/> is negative.
    /// </exception>
    public SimulatedSource(double recordsPerSecond, int batchSize, Func<long, Batch<T>, T> generate, int payloadBytesPerBatch = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadBytesPerBatch);
        ArgumentNullException.ThrowIfNull(generate);

        RecordsPerSecond = recordsPerSecond;
        BatchSize = batchSize;
        _generate = generate;
        _payloadBytesPerBatch = payloadBytesPerBatch;
    }

    /// <summary>The average rate records are produced at.</summary>
    public double RecordsPerSecond { get; }

    /// <summary>How many records go in each batch.</summary>
    public int BatchSize { get; }

    /// <inheritdoc/>
    /// <remarks>Runs until cancelled; a simulator never runs out of data.</remarks>
    public async Task RunAsync(IBatchWriter<T> writer, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(writer);

        long produced = 0;
        long sequence = 0;
        long start = Stopwatch.GetTimestamp();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            double elapsedSeconds = Stopwatch.GetElapsedTime(start).TotalSeconds;
            long due = (long)(elapsedSeconds * RecordsPerSecond);

            if (due - produced < BatchSize)
            {
                // Not a full batch behind yet. Sleep briefly. (On Windows the shortest real sleep
                // is about 15 ms, so batches come out in small bursts; the average rate is exact.)
                await Task.Delay(1, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var batch = new Batch<T>(BatchSize, sequence++, _payloadBytesPerBatch);
            Span<T> buffer = batch.Buffer;

            for (int i = 0; i < BatchSize; i++)
            {
                buffer[i] = _generate(produced + i, batch);
            }

            batch.SetCount(BatchSize);
            produced += BatchSize;

            // Ownership passes to the writer here, whatever happens.
            await writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    // Adapts a flat generator to the payload-aware form. A separate method because a constructor
    // can't run code before calling the other constructor.
    private static Func<long, Batch<T>, T> Wrap(Func<long, T> generate)
    {
        ArgumentNullException.ThrowIfNull(generate);
        return (index, _) => generate(index);
    }
}
