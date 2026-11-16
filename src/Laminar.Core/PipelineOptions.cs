namespace Laminar.Core;

/// <summary>
/// The sizing and policy of a pipeline, fixed when it is prepared (ADR 0005).
/// </summary>
/// <remarks>
/// <see cref="Policy"/> and <see cref="BatchCapacity"/> must always be given (there is no sensible
/// default for either); the rest have defaults. Wrong sizing never breaks a run: anything that
/// has to be allocated after prepare is counted in <see cref="PipelineMetrics"/>.
/// </remarks>
public sealed record PipelineOptions
{
    /// <summary>What the buffer does when it is full. There is deliberately no default (ADR 0002).</summary>
    public required FullBufferPolicy Policy { get; init; }

    /// <summary>
    /// Records per batch. The source must not put more than this in one batch.
    /// </summary>
    public required int BatchCapacity { get; init; }

    /// <summary>How many batches the buffer holds before <see cref="Policy"/> applies. Default 64.</summary>
    public int BufferCapacity { get; init; } = 64;

    /// <summary>
    /// Payload bytes each batch starts with, for nested data (ADR 0004). Size it for the largest
    /// batch you expect; 0 (the default) means records have no nested data.
    /// </summary>
    public int PayloadBytesPerBatch { get; init; }

    /// <summary>
    /// How many records the store should make room for in <c>Prepare</c>. 0 (the default) reserves
    /// nothing, and the store grows as records arrive.
    /// </summary>
    public long ReserveRecords { get; init; }

    /// <summary>
    /// How many batches the pool needs: the buffer full, one held by the worker, and one being
    /// filled by the source.
    /// </summary>
    public int PoolSize => BufferCapacity + 2;
}
