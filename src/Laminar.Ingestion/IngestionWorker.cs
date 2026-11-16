using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// The single reader of a <see cref="BoundedBuffer{T}"/> (ADR 0002): takes each batch, appends its
/// records to the store, and returns the batch's memory to the pool.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>One worker per buffer.</b> Running one worker keeps batches in order and lets the store
/// assume a single writer.
/// </para>
/// <para>
/// <b>Stopping:</b> call <see cref="BoundedBuffer{T}.Complete"/> for a graceful stop: the worker
/// appends everything already buffered, then <see cref="RunAsync"/> returns. Cancelling the token
/// is an abort: the worker stops at once and batches still in the buffer are not appended.
/// </para>
/// <para>
/// <b>Failure:</b> if the store throws, the worker completes the buffer with that error, so
/// sources stop instead of waiting forever for room, and then rethrows. Recording fails loudly,
/// never silently.
/// </para>
/// </remarks>
public sealed class IngestionWorker<T>
{
    private readonly BoundedBuffer<T> _buffer;
    private readonly IRecordStore<T> _store;

    // Guards the two counters so a snapshot never sees a batch counted but not its records.
    private readonly object _gate = new();
    private long _batchesAppended;
    private long _recordsAppended;

    /// <summary>
    /// Creates a worker that moves batches from <paramref name="buffer"/> into <paramref name="store"/>.
    /// </summary>
    public IngestionWorker(BoundedBuffer<T> buffer, IRecordStore<T> store)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(store);

        _buffer = buffer;
        _store = store;
    }

    /// <summary>
    /// Runs until the buffer is completed and drained, or until cancelled.
    /// </summary>
    /// <param name="cancellationToken">Aborts the worker without draining the buffer.</param>
    /// <exception cref="OperationCanceledException">The worker was cancelled.</exception>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Batch<T>? batch = await _buffer.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (batch is null)
            {
                return;
            }

            using (batch)
            {
                try
                {
                    _store.Append(batch);
                }
                catch (Exception ex)
                {
                    _buffer.Complete(ex);
                    throw;
                }

                lock (_gate)
                {
                    _batchesAppended++;
                    _recordsAppended += batch.Count;
                }
            }
        }
    }

    /// <summary>
    /// Returns a consistent copy of the buffer's and the worker's counters together.
    /// </summary>
    /// <remarks>
    /// The worker's lock is held while the buffer is read. The worker can't record an append in
    /// that time, and it can't take another batch until it has, so at most one batch shows as
    /// in flight. Reading the two one after the other could show two: the worker could finish one
    /// batch and take the next in between.
    /// </remarks>
    public PipelineMetrics GetMetrics()
    {
        lock (_gate)
        {
            // Lock order is always worker, then buffer; the buffer never takes the worker's lock,
            // so this can't deadlock.
            BoundedBufferCounters buffer = _buffer.GetCounters();

            return new PipelineMetrics(
                BatchesWritten: buffer.BatchesWritten,
                RecordsWritten: buffer.RecordsWritten,
                BatchesDropped: buffer.BatchesDropped,
                RecordsDropped: buffer.RecordsDropped,
                BatchesAppended: _batchesAppended,
                RecordsAppended: _recordsAppended,
                BatchesBuffered: buffer.BatchesBuffered,
                RecordsBuffered: buffer.RecordsBuffered,
                BatchesInFlight: buffer.BatchesTaken - _batchesAppended,
                RecordsInFlight: buffer.RecordsTaken - _recordsAppended,
                BufferCapacity: buffer.Capacity);
        }
    }
}
