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
    where T : unmanaged
{
    private readonly Func<long, Batch<T>, T> _generate;

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
    /// <remarks>
    /// How much payload each batch starts with is the pipeline's sizing
    /// (<see cref="PipelineOptions.PayloadBytesPerBatch"/>), not the source's.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The rate or the batch size is zero or negative.</exception>
    public SimulatedSource(double recordsPerSecond, int batchSize, Func<long, Batch<T>, T> generate)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentNullException.ThrowIfNull(generate);

        RecordsPerSecond = recordsPerSecond;
        BatchSize = batchSize;
        _generate = generate;
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

            // From the pipeline's prepared pool: no allocation here (ADR 0005).
            Batch<T> batch = writer.RentBatch(sequence++);

            try
            {
                if (batch.Capacity < BatchSize)
                {
                    throw new InvalidOperationException(
                        $"The source fills batches of {BatchSize} records, but the pipeline's batches hold " +
                        $"{batch.Capacity}. Set PipelineOptions.BatchCapacity to at least {BatchSize}.");
                }

                Span<T> buffer = batch.Buffer;
                for (int i = 0; i < BatchSize; i++)
                {
                    buffer[i] = _generate(produced + i, batch);
                }

                batch.SetCount(BatchSize);
            }
            catch
            {
                // Never pushed, so still ours: give it back to the pool.
                batch.Dispose();
                throw;
            }

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
