namespace Laminar.Core;

/// <summary>
/// Where ingestion delivers records: the end of the pipeline.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// Storage is pluggable (ADR 0001). This contract holds only what every store needs; each store
/// adds its own way of reading, since rows are read by index and columns by field.
/// </para>
/// <para>
/// <b>Threading:</b> <see cref="Append"/> is called from a single ingestion thread.
/// <see cref="Count"/> may be read from any thread, so implementations must make that read safe.
/// </para>
/// </remarks>
public interface IRecordStore<T>
    where T : unmanaged
{
    /// <summary>
    /// Allocates room for at least <paramref name="records"/> records in advance, so they can be
    /// appended without allocating (ADR 0005). Called by the pipeline in <c>Prepare</c>, before
    /// any append. A store that reserves nothing may do nothing.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="records"/> is negative.</exception>
    void Reserve(long records);

    /// <summary>
    /// How many records the store holds. A <see cref="long"/>, because a long run can exceed
    /// <see cref="int.MaxValue"/> records.
    /// </summary>
    long Count { get; }

    /// <summary>
    /// Copies the batch's records, and any payload they point at, into the store.
    /// </summary>
    /// <param name="batch">
    /// The batch to add, read-only. The whole batch is passed, not just its records, so a store
    /// can resolve the records' <see cref="PayloadSlice{TItem}"/> values (ADR 0004). The store must
    /// copy what it needs before returning and never keep the batch or any span from it: the
    /// caller disposes the batch afterwards, and its memory goes back to the pool.
    /// </param>
    void Append(Batch<T> batch);
}
