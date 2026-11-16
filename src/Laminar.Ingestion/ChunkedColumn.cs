namespace Laminar.Ingestion;

/// <summary>
/// A growable, append-only sequence of values stored in fixed-size chunks that never move: the
/// building block of every store. A row table is one column of whole items; a column table is one
/// column per field.
/// </summary>
/// <typeparam name="TItem">The value type. Must be <see langword="unmanaged"/> (ADR 0003).</typeparam>
/// <remarks>
/// <para>
/// <b>Never moved:</b> when a chunk fills, a new one is added. Existing chunks are never copied, so
/// growing costs one allocation per chunk and never needs twice the memory.
/// </para>
/// <para>
/// <b>Write, then publish:</b> the store (one writer thread) adds values, then calls
/// <c>Publish</c> to make them visible. Readers on any thread see only published values, without
/// locks: <see cref="Count"/> is published last, so every value below it is already in place.
/// </para>
/// <para>
/// <b>Reading in bulk:</b> <see cref="GetChunk"/> hands out a whole chunk as a span. Scanning a
/// column chunk by chunk is the fastest way to read it: memory is contiguous, so the CPU's cache
/// and SIMD instructions work at full speed.
/// </para>
/// </remarks>
public sealed class ChunkedColumn<TItem> : IReservable
    where TItem : unmanaged
{
    /// <summary>The chunk size used when none is given: 65,536 values.</summary>
    public const int DefaultChunkSize = 1 << 16;

    // The list of chunks. Replaced, never edited in place for existing entries, when it runs out
    // of slots, so a reader holding the old list still sees valid chunks.
    private TItem[][] _chunks = new TItem[4][];
    private int _chunkCount;

    // Published to readers; written only with Volatile.Write.
    private long _count;

    // Writer-only state: how many values are written (published or not), and where the next
    // one goes.
    private long _written;
    private TItem[]? _writeChunk;
    private int _writeChunkIndex;
    private int _writeOffset;

    /// <summary>
    /// Creates an empty column.
    /// </summary>
    /// <param name="chunkSize">How many values each chunk holds.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is zero or negative.</exception>
    public ChunkedColumn(int chunkSize = DefaultChunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        ChunkSize = chunkSize;
    }

    /// <summary>How many values each chunk holds.</summary>
    public int ChunkSize { get; }

    /// <summary>How many values are published and readable.</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>How many chunks hold published values. The last one may be partly filled.</summary>
    public int ChunkCount => (int)((Count + ChunkSize - 1) / ChunkSize);

    /// <summary>
    /// Returns a copy of the value at <paramref name="index"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is negative, or not less than <see cref="Count"/>.
    /// </exception>
    public TItem this[long index]
    {
        get
        {
            long count = Volatile.Read(ref _count);
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, count);

            TItem[][] chunks = Volatile.Read(ref _chunks);
            return chunks[index / ChunkSize][index % ChunkSize];
        }
    }

    /// <summary>
    /// Returns the published values of one chunk, read-only. Chunk <c>i</c> holds values
    /// <c>i Ã— ChunkSize</c> onwards.
    /// </summary>
    /// <remarks>
    /// A view of the chunk; nothing is copied. Safe to keep while the column exists, because chunks
    /// never move and published values never change.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="chunkIndex"/> is negative, or not less than <see cref="ChunkCount"/>.
    /// </exception>
    public ReadOnlySpan<TItem> GetChunk(int chunkIndex)
    {
        long count = Volatile.Read(ref _count);
        long start = (long)chunkIndex * ChunkSize;
        ArgumentOutOfRangeException.ThrowIfNegative(chunkIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(start, count, nameof(chunkIndex));

        TItem[][] chunks = Volatile.Read(ref _chunks);
        int length = (int)Math.Min(ChunkSize, count - start);
        return chunks[chunkIndex].AsSpan(0, length);
    }

    /// <summary>
    /// Copies values starting at <paramref name="start"/> into <paramref name="destination"/>,
    /// filling it completely.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="start"/> is negative, or the range runs past <see cref="Count"/>.
    /// </exception>
    public void CopyTo(long start, Span<TItem> destination)
    {
        long count = Volatile.Read(ref _count);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(start + destination.Length, count, nameof(destination));

        TItem[][] chunks = Volatile.Read(ref _chunks);
        long position = start;

        while (!destination.IsEmpty)
        {
            int chunkIndex = (int)(position / ChunkSize);
            int offset = (int)(position % ChunkSize);

            ReadOnlySpan<TItem> available = chunks[chunkIndex].AsSpan(offset);
            int n = Math.Min(destination.Length, available.Length);

            available[..n].CopyTo(destination);
            destination = destination[n..];
            position += n;
        }
    }

    // ---- Writer side: one thread only, and only stores inside this library call these. ----

    /// <summary>How many values are written, published or not. Writer thread only.</summary>
    internal long Written => _written;

    /// <summary>Adds one value, not yet visible to readers.</summary>
    internal void Add(TItem item)
    {
        if (_writeChunk is null || _writeOffset == ChunkSize)
        {
            AdvanceChunk();
        }

        _writeChunk![_writeOffset++] = item;
        _written++;
    }

    /// <summary>Adds values in bulk, not yet visible to readers.</summary>
    internal void AddRange(ReadOnlySpan<TItem> items)
    {
        while (!items.IsEmpty)
        {
            if (_writeChunk is null || _writeOffset == ChunkSize)
            {
                AdvanceChunk();
            }

            Span<TItem> free = _writeChunk.AsSpan(_writeOffset);
            int n = Math.Min(items.Length, free.Length);

            items[..n].CopyTo(free);
            items = items[n..];
            _writeOffset += n;
            _written += n;
        }
    }

    /// <summary>
    /// Allocates chunks in advance until the column has room for at least
    /// <paramref name="count"/> values in total, so appending up to that point allocates nothing.
    /// </summary>
    internal void Reserve(long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        // Chunks are only added at the end, and the write position moves onto them the normal way
        // (AdvanceChunk finds the chunk already there instead of adding one).
        while ((long)_chunkCount * ChunkSize < count)
        {
            AddChunk();
        }
    }

    // Lets a store reserve a column it only knows as an object, without knowing its element type.
    void IReservable.Reserve(long count) => Reserve(count);

    /// <summary>Makes every value written so far visible to readers.</summary>
    internal void Publish() => Volatile.Write(ref _count, _written);

    /// <summary>
    /// Forgets values written since the last <see cref="Publish"/>, as if they were never added.
    /// Used when an append fails halfway, so the column stays in step with the others.
    /// </summary>
    internal void Rollback()
    {
        _written = _count;
        _writeChunkIndex = (int)(_written / ChunkSize);
        _writeOffset = (int)(_written % ChunkSize);
        _writeChunk = _writeChunkIndex < _chunkCount ? _chunks[_writeChunkIndex] : null;
    }

    // Moves the write position to the start of the next chunk, adding a chunk if needed.
    private void AdvanceChunk()
    {
        if (_writeChunk is not null)
        {
            _writeChunkIndex++;
        }

        _writeOffset = 0;

        if (_writeChunkIndex == _chunkCount)
        {
            AddChunk();
        }

        _writeChunk = _chunks[_writeChunkIndex];
    }

    private void AddChunk()
    {
        TItem[][] chunks = _chunks;

        if (_chunkCount == chunks.Length)
        {
            // Out of slots: copy the chunk references (not the values) into a list twice as big,
            // then swap it in. Readers holding the old list are unaffected.
            var bigger = new TItem[chunks.Length * 2][];
            Array.Copy(chunks, bigger, _chunkCount);
            Volatile.Write(ref _chunks, bigger);
            chunks = bigger;
        }

        // One allocation per chunk, not per value. Chunks live as long as the column, so they are
        // not rented from the pool. "Uninitialized" skips zeroing memory that appends overwrite;
        // readers never see it, because they stay below the published count.
        chunks[_chunkCount] = GC.AllocateUninitializedArray<TItem>(ChunkSize);
        _chunkCount++;
    }
}

/// <summary>Something that can make room in advance. Lets stores reserve columns of any element type.</summary>
internal interface IReservable
{
    void Reserve(long count);
}
