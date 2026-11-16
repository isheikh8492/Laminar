namespace Laminar.Core;

/// <summary>
/// What a source uses to get empty batches and push full ones. A pipeline hands one to its source.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public interface IBatchWriter<T>
    where T : unmanaged
{
    /// <summary>
    /// Takes an empty batch from the pipeline's prepared pool (ADR 0005), ready to fill. Its
    /// capacity is the pipeline's batch capacity, and its payload buffer is already sized.
    /// </summary>
    /// <param name="sequence">
    /// The batch's position in the source's stream: 0 for the first batch, increasing by 1 each time.
    /// </param>
    /// <returns>
    /// A batch the caller now owns. Push it with <see cref="WriteAsync"/>, or dispose it to give it
    /// back unused.
    /// </returns>
    Batch<T> RentBatch(long sequence);

    /// <summary>
    /// Pushes a batch into the pipeline, applying the buffer's <see cref="FullBufferPolicy"/>.
    /// </summary>
    /// <param name="batch">
    /// The batch to push. Ownership passes to the writer on every call, even if the batch is
    /// dropped: the caller must not use or dispose it afterwards.
    /// </param>
    /// <param name="cancellationToken">Stops waiting for room under <see cref="FullBufferPolicy.Wait"/>.</param>
    /// <returns>
    /// A task that completes once the batch has been accepted or dropped. It completes immediately
    /// under the drop policies, or when there is room; it only waits under
    /// <see cref="FullBufferPolicy.Wait"/> with a full buffer.
    /// </returns>
    ValueTask WriteAsync(Batch<T> batch, CancellationToken cancellationToken = default);
}
