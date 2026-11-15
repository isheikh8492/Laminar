namespace Laminar.Core;

/// <summary>
/// A point-in-time copy of the pipeline's counters.
/// </summary>
/// <remarks>
/// <para>
/// All values are taken at the same moment, so they add up. Reading live counters one at a time
/// could mix numbers from different moments.
/// </para>
/// <para>
/// <b>Health check:</b> every batch written must be accounted for:
/// <c>BatchesWritten == BatchesAppended + BatchesDropped + BatchesBuffered</c>.
/// If this ever fails, data was lost silently.
/// </para>
/// </remarks>
/// <param name="BatchesWritten">Batches pushed by sources.</param>
/// <param name="RecordsWritten">Records in the batches pushed by sources.</param>
/// <param name="BatchesDropped">Batches dropped by the full-buffer policy.</param>
/// <param name="RecordsDropped">Records in the dropped batches.</param>
/// <param name="BatchesAppended">Batches whose records were appended to the store.</param>
/// <param name="RecordsAppended">Records appended to the store.</param>
/// <param name="BatchesBuffered">Batches currently waiting in the buffer.</param>
/// <param name="BufferCapacity">How many batches the buffer can hold.</param>
public readonly record struct PipelineMetrics(
    long BatchesWritten,
    long RecordsWritten,
    long BatchesDropped,
    long RecordsDropped,
    long BatchesAppended,
    long RecordsAppended,
    int BatchesBuffered,
    int BufferCapacity);
