namespace Laminar.Ingestion;

/// <summary>
/// A consistent point-in-time copy of a <see cref="BoundedBuffer{T}"/>'s counters.
/// </summary>
/// <remarks>
/// All values are taken under the buffer's lock, so they always add up:
/// <c>BatchesWritten == BatchesDropped + BatchesTaken + BatchesBuffered</c>.
/// </remarks>
/// <param name="BatchesWritten">Batches handed to <see cref="BoundedBuffer{T}.WriteAsync"/> and accepted or dropped.</param>
/// <param name="RecordsWritten">Records in those batches.</param>
/// <param name="BatchesDropped">Batches dropped by the full-buffer policy or by a cancelled or rejected write.</param>
/// <param name="RecordsDropped">Records in the dropped batches.</param>
/// <param name="BatchesTaken">Batches handed to the reader by <see cref="BoundedBuffer{T}.ReadAsync"/>.</param>
/// <param name="RecordsTaken">Records in the batches handed to the reader.</param>
/// <param name="Capacity">How many batches the buffer can hold.</param>
public readonly record struct BoundedBufferCounters(
    long BatchesWritten,
    long RecordsWritten,
    long BatchesDropped,
    long RecordsDropped,
    long BatchesTaken,
    long RecordsTaken,
    int Capacity)
{
    /// <summary>Batches currently waiting in the buffer.</summary>
    public long BatchesBuffered => BatchesWritten - BatchesDropped - BatchesTaken;

    /// <summary>Records currently waiting in the buffer.</summary>
    public long RecordsBuffered => RecordsWritten - RecordsDropped - RecordsTaken;
}
