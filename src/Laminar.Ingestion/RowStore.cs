using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// The first store (ADR 0001): records kept whole, one after another, in arrival order. Suited to
/// per-record reads such as an oscilloscope trace or a record-by-record replay.
/// </summary>
/// <typeparam name="T">
/// The record type. Must be <see langword="unmanaged"/> (ADR 0003): plain values with no
/// references inside, so a block of records is just bytes and can be copied in one go.
/// </typeparam>
/// <remarks>
/// <para>
/// <b>Chunked, never moved:</b> records live in fixed-size chunks. When a chunk fills, a new one is
/// added; existing chunks are never copied or reallocated. Growing a single big array would copy
/// every record each time it doubled, and briefly need twice the memory.
/// </para>
/// <para>
/// <b>Threading:</b> one writer (the ingestion worker) calls <see cref="Append"/>. Any number of
/// readers may call <see cref="Count"/>, the indexer and <see cref="CopyTo"/> at the same time,
/// without locks. This is safe because the writer fills records first and publishes the new
/// <see cref="Count"/> last, and readers never look past the count they read.
/// </para>
/// </remarks>
public sealed class RowStore<T> : IRecordStore<T>
    where T : unmanaged
{
    /// <summary>The chunk size used when none is given: 65,536 records.</summary>
    public const int DefaultChunkSize = 1 << 16;

    // The list of chunks. Replaced (never edited in place for existing entries) when it runs
    // out of slots, so a reader holding the old list still sees valid chunks.
    private T[][] _chunks;

    // Chunks allocated so far. Only the writer uses this.
    private int _chunkCount;

    // Records published to readers. Written only by the writer, with Volatile.Write.
    private long _count;

    /// <summary>
    /// Creates an empty store.
    /// </summary>
    /// <param name="chunkSize">How many records each chunk holds.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is zero or negative.</exception>
    public RowStore(int chunkSize = DefaultChunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);

        ChunkSize = chunkSize;
        _chunks = new T[4][];
    }

    /// <summary>How many records each chunk holds.</summary>
    public int ChunkSize { get; }

    /// <inheritdoc/>
    public long Count => Volatile.Read(ref _count);

    /// <summary>
    /// Returns a copy of the record at <paramref name="index"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is negative, or not less than <see cref="Count"/>.
    /// </exception>
    public T this[long index]
    {
        get
        {
            long count = Volatile.Read(ref _count);
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, count);

            T[][] chunks = Volatile.Read(ref _chunks);
            return chunks[index / ChunkSize][index % ChunkSize];
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Stores the records only. Payload slices inside them are kept as plain numbers, which mean
    /// nothing once the batch is gone; records with payload need a layout store (ADR 0004).
    /// Must only be called from one thread at a time.
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
        // Only this thread writes _count, so a plain read is enough here.
        long count = _count;

        while (!records.IsEmpty)
        {
            int chunkIndex = (int)(count / ChunkSize);
            int offset = (int)(count % ChunkSize);

            if (chunkIndex == _chunkCount)
            {
                AddChunk();
            }

            Span<T> free = _chunks[chunkIndex].AsSpan(offset);
            int n = Math.Min(records.Length, free.Length);

            records[..n].CopyTo(free);
            records = records[n..];
            count += n;
        }

        // Publish last: once a reader sees the new count, every record below it is in place.
        Volatile.Write(ref _count, count);
    }

    /// <summary>
    /// Copies records starting at <paramref name="start"/> into <paramref name="destination"/>,
    /// filling it completely. Use this to read a window of records, for example for a plot.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="start"/> is negative, or the range runs past <see cref="Count"/>.
    /// </exception>
    public void CopyTo(long start, Span<T> destination)
    {
        long count = Volatile.Read(ref _count);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(start + destination.Length, count, nameof(destination));

        T[][] chunks = Volatile.Read(ref _chunks);
        long position = start;

        while (!destination.IsEmpty)
        {
            int chunkIndex = (int)(position / ChunkSize);
            int offset = (int)(position % ChunkSize);

            ReadOnlySpan<T> available = chunks[chunkIndex].AsSpan(offset);
            int n = Math.Min(destination.Length, available.Length);

            available[..n].CopyTo(destination);
            destination = destination[n..];
            position += n;
        }
    }

    private void AddChunk()
    {
        T[][] chunks = _chunks;

        if (_chunkCount == chunks.Length)
        {
            // Out of slots: copy the chunk references (not the records) into a list twice as big,
            // then swap it in. Readers holding the old list are unaffected.
            T[][] bigger = new T[chunks.Length * 2][];
            Array.Copy(chunks, bigger, _chunkCount);
            Volatile.Write(ref _chunks, bigger);
            chunks = bigger;
        }

        // One allocation per chunk, not per record. Chunks live as long as the store, so they are
        // not rented from the pool.
        chunks[_chunkCount] = new T[ChunkSize];
        _chunkCount++;
    }
}
