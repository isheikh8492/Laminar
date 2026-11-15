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
{
    /// <summary>
    /// How many records the store holds. A <see cref="long"/>, because a long run can exceed
    /// <see cref="int.MaxValue"/> records.
    /// </summary>
    long Count { get; }

    /// <summary>
    /// Copies <paramref name="records"/> into the store.
    /// </summary>
    /// <param name="records">
    /// The records to add. The store must copy them before returning and never keep the span,
    /// because the memory behind it goes back to the pool afterwards.
    /// </param>
    void Append(ReadOnlySpan<T> records);
}
