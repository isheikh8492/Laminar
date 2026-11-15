namespace Laminar.Core;

/// <summary>
/// Something that produces data and pushes it into the pipeline in batches: hardware, a socket,
/// a file, or a simulator.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// Sources push (ADR 0002): whenever data arrives, the source fills a <see cref="Batch{T}"/> and
/// passes it to the writer. A source's job is to adapt whatever arrives into batches of
/// <typeparamref name="T"/>.
/// </remarks>
public interface ISource<T>
{
    /// <summary>
    /// Produces batches and pushes them to <paramref name="writer"/> until there is no more data or
    /// cancellation is requested.
    /// </summary>
    /// <param name="writer">Where to push each batch. Ownership of each batch passes to it.</param>
    /// <param name="cancellationToken">Signals that the source should stop.</param>
    /// <returns>
    /// A task that completes when the source has finished. Completing normally means the source
    /// ran out of data or was stopped; throwing means it failed.
    /// </returns>
    Task RunAsync(IBatchWriter<T> writer, CancellationToken cancellationToken);
}
