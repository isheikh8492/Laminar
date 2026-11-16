using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// The simplest store (ADR 0001): records kept whole, one after another, in arrival order. Suited
/// to per-record reads such as an oscilloscope trace or a record-by-record replay.
/// </summary>
/// <typeparam name="T">
/// The record type. Must be <see langword="unmanaged"/> (ADR 0003): plain values with no
/// references inside, so a block of records is just bytes and can be copied in one go.
/// </typeparam>
/// <remarks>
/// <para>
/// One <see cref="ChunkedColumn{TItem}"/> of whole records: chunks that never move, appended in
/// bulk. For records with nested data, or to store fields as columns, use
/// <see cref="Layout.LayoutStore{T}"/>.
/// </para>
/// <para>
/// <b>Threading:</b> one writer (the ingestion worker) calls <see cref="Append(Batch{T})"/>. Any
/// number of readers may use <see cref="Count"/>, the indexer, <see cref="CopyTo"/> and
/// <see cref="Records"/> at the same time, without locks.
/// </para>
/// </remarks>
public sealed class RowStore<T> : IRecordStore<T>
    where T : unmanaged
{
    private readonly ChunkedColumn<T> _records;

    /// <summary>
    /// Creates an empty store.
    /// </summary>
    /// <param name="chunkSize">How many records each chunk holds.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is zero or negative.</exception>
    public RowStore(int chunkSize = ChunkedColumn<T>.DefaultChunkSize)
    {
        _records = new ChunkedColumn<T>(chunkSize);
    }

    /// <summary>How many records each chunk holds.</summary>
    public int ChunkSize => _records.ChunkSize;

    /// <inheritdoc/>
    public long Count => _records.Count;

    /// <summary>The records, for bulk reading chunk by chunk.</summary>
    public ChunkedColumn<T> Records => _records;

    /// <summary>
    /// Returns a copy of the record at <paramref name="index"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is negative, or not less than <see cref="Count"/>.
    /// </exception>
    public T this[long index] => _records[index];

    /// <inheritdoc/>
    /// <remarks>Must be called from the writer's thread, or before appending starts.</remarks>
    public void Reserve(long records) => _records.Reserve(records);

    /// <inheritdoc/>
    /// <remarks>
    /// Stores the records only. Payload slices inside them are kept as plain numbers, which mean
    /// nothing once the batch is gone; records with payload need a <see cref="Layout.LayoutStore{T}"/>
    /// (ADR 0004). Must only be called from one thread at a time.
    /// </remarks>
    public void Append(Batch<T> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        Append(batch.Records);
    }

    /// <summary>
    /// Copies <paramref name="records"/> into the store.
    /// </summary>
    /// <remarks>Must only be called from one thread at a time.</remarks>
    public void Append(ReadOnlySpan<T> records)
    {
        _records.AddRange(records);
        _records.Publish();
    }

    /// <summary>
    /// Copies records starting at <paramref name="start"/> into <paramref name="destination"/>,
    /// filling it completely. Use this to read a window of records, for example for a plot.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="start"/> is negative, or the range runs past <see cref="Count"/>.
    /// </exception>
    public void CopyTo(long start, Span<T> destination) => _records.CopyTo(start, destination);
}
