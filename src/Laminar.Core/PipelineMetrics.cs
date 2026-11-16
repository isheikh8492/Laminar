namespace Laminar.Core;

/// <summary>
/// A point-in-time copy of the pipeline's counters.
/// </summary>
/// <remarks>
/// <para>
/// Every batch written is in exactly one place, so the numbers always add up:
/// <c>Written == Appended + Dropped + Buffered + InFlight</c>, for batches and for records alike.
/// </para>
/// <para>
/// <b>In flight</b> means taken from the buffer by the ingestion worker but not yet appended to the
/// store: 0 or 1 batch while running. A negative value, or more than 1, means a bug.
/// </para>
/// <para>
/// <b>Health check after stopping:</b> nothing is buffered or in flight, so
/// <c>RecordsWritten == RecordsAppended + RecordsDropped</c>, and the store's count equals
/// <see cref="RecordsAppended"/>. If either fails, data was lost silently.
/// </para>
/// </remarks>
/// <param name="BatchesWritten">Batches pushed by sources.</param>
/// <param name="RecordsWritten">Records in the batches pushed by sources.</param>
/// <param name="BatchesDropped">
/// Batches dropped: by the full-buffer policy, by a cancelled or rejected write, or left in the
/// buffer when the pipeline stopped.
/// </param>
/// <param name="RecordsDropped">Records in the dropped batches.</param>
/// <param name="BatchesAppended">Batches whose records were appended to the store.</param>
/// <param name="RecordsAppended">Records appended to the store.</param>
/// <param name="BatchesBuffered">Batches currently waiting in the buffer.</param>
/// <param name="RecordsBuffered">Records in the waiting batches.</param>
/// <param name="BatchesInFlight">
/// Batches taken by the worker but not yet appended. After a store failure, includes the batch
/// that failed.
/// </param>
/// <param name="RecordsInFlight">Records in the batches in flight.</param>
/// <param name="BufferCapacity">How many batches the buffer can hold.</param>
public readonly record struct PipelineMetrics(
    long BatchesWritten,
    long RecordsWritten,
    long BatchesDropped,
    long RecordsDropped,
    long BatchesAppended,
    long RecordsAppended,
    long BatchesBuffered,
    long RecordsBuffered,
    long BatchesInFlight,
    long RecordsInFlight,
    int BufferCapacity)
{
    /// <summary>
    /// How full the buffer is, from 0 (empty) to 1 (full). The key health metric (ADR 0002): a
    /// buffer that stays near 1 means ingestion can't keep up with the sources.
    /// </summary>
    public double BufferFill => BufferCapacity == 0 ? 0 : (double)BatchesBuffered / BufferCapacity;
}
