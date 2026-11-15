namespace Laminar.Core;

/// <summary>
/// Where a source pushes its batches. Implemented by the bounded buffer.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public interface IBatchWriter<T>
{
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
