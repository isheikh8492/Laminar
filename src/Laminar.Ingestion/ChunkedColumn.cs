namespace Laminar.Ingestion;

/// <summary>
/// A growable, append-only sequence of values stored in fixed-size chunks that never move: the
/// building block of every store. A row table is one column of whole items; a column table is one
/// column per field.
/// </summary>
/// <typeparam name="TItem">The value type. Must be <see langword="unmanaged"/> (ADR 0005).</typeparam>
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
/// <b>Retention (ADR 0006):</b> the store can free the oldest chunks. Indices stay absolute: value
/// 1,000,000 keeps that index for the whole run. <see cref="FirstRetained"/> is the oldest value
/// still readable; reading below it reports the value as evicted, never another value. A reader
/// that already holds a chunk keeps it valid: freed chunks are left to the garbage collector,
/// never reused while a reader might see them.
/// </para>
/// <para>
/// <b>Reading in bulk:</b> <see cref="GetChunk"/> hands out a chunk as a span. Scanning a column
/// chunk by chunk, from <see cref="FirstChunk"/> to <see cref="ChunkCount"/>, is the fastest way to
/// read it: memory is contiguous, so the CPU's cache and SIMD instructions work at full speed.
/// </para>
/// </remarks>
public sealed class ChunkedColumn<TItem> : IColumn
    where TItem : unmanaged
{
    /// <summary>The chunk size used when none is given: 65,536 values.</summary>
    public const int DefaultChunkSize = 1 << 16;

    // The chunks in memory, in a ring: chunk c sits in slot c mod the ring's length. A slot holds
    // either nothing or one live chunk; the ring grows (as a new array, swapped in) whenever two
    // live chunks would need the same slot. Each chunk carries its own index, so a reader can
    // check it found the chunk it asked for and not a newer one in the same slot.
    private Chunk?[] _slots = new Chunk?[4];

    // Published to readers; written only with Volatile.Write.
    private long _count;
    private long _firstRetained;

    // Writer-only state: how many values are written (published or not), and where the next one goes.
    private long _written;
    private Chunk? _writeChunk;
    private int _writeOffset;

    // Writer-only: the lowest chunk index that may still be in the ring.
    private long _firstLiveChunk;

    // Writer-only: chunks allocated in advance by Reserve, not yet given an index.
    private readonly Stack<Chunk> _spare = new();

    private long _chunksAllocated;
    private long _chunksFreed;

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

    /// <summary>How many values have been published, ever. Also the index the next value will get.</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>
    /// The oldest value still in memory. Values from here up to <see cref="Count"/> are readable;
    /// values below it have been evicted (ADR 0006). Zero unless the store frees old data.
    /// </summary>
    public long FirstRetained => Volatile.Read(ref _firstRetained);

    /// <summary>The index of the first chunk still in memory: the one holding <see cref="FirstRetained"/>.</summary>
    public long FirstChunk => FirstRetained / ChunkSize;

    /// <summary>
    /// One past the index of the last chunk holding published values. Chunks
    /// <see cref="FirstChunk"/> up to (not including) this are readable; the last may be partly filled.
    /// </summary>
    public long ChunkCount => (Count + ChunkSize - 1) / ChunkSize;

    /// <summary>How many chunks this column has allocated, including those reserved in advance.</summary>
    public long ChunksAllocated => Volatile.Read(ref _chunksAllocated);

    /// <summary>How many chunks this column has freed because they fell out of its retention window.</summary>
    public long ChunksFreed => Volatile.Read(ref _chunksFreed);

    /// <summary>
    /// Returns a copy of the value at <paramref name="index"/>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is negative, not less than <see cref="Count"/>, or has been evicted
    /// (it is below <see cref="FirstRetained"/>).
    /// </exception>
    public TItem this[long index]
    {
        get
        {
            // Count first, then the ring: the ring is then at least as new as the published count.
            long count = Volatile.Read(ref _count);
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, count);

            TItem[] values = FindChunk(index / ChunkSize, index);
            return values[index % ChunkSize];
        }
    }

    /// <summary>
    /// Returns the readable values of one chunk, read-only. Chunk <c>i</c> holds values from
    /// <c>i × ChunkSize</c>, except that the first chunk in memory starts at
    /// <see cref="FirstRetained"/> instead.
    /// </summary>
    /// <remarks>
    /// A view of the chunk; nothing is copied. Safe to keep while you use it, even if the chunk is
    /// evicted meanwhile: published values never change, and a freed chunk is never reused.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="chunkIndex"/> is below <see cref="FirstChunk"/> (evicted), or not less
    /// than <see cref="ChunkCount"/>.
    /// </exception>
    public ReadOnlySpan<TItem> GetChunk(long chunkIndex)
    {
        long count = Volatile.Read(ref _count);
        long first = Volatile.Read(ref _firstRetained);
        ArgumentOutOfRangeException.ThrowIfNegative(chunkIndex);

        long chunkStart = chunkIndex * ChunkSize;
        long start = Math.Max(chunkStart, first);
        long end = Math.Min(chunkStart + ChunkSize, count);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(chunkStart, count, nameof(chunkIndex));
        if (start >= end)
        {
            throw Evicted(chunkStart);
        }

        TItem[] values = FindChunk(chunkIndex, start);
        return values.AsSpan((int)(start - chunkStart), (int)(end - start));
    }

    /// <summary>
    /// Copies values starting at <paramref name="start"/> into <paramref name="destination"/>,
    /// filling it completely.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="start"/> is negative or evicted, or the range runs past <see cref="Count"/>.
    /// </exception>
    public void CopyTo(long start, Span<TItem> destination)
    {
        long count = Volatile.Read(ref _count);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(start + destination.Length, count, nameof(destination));

        long position = start;
        while (!destination.IsEmpty)
        {
            long chunkIndex = position / ChunkSize;
            int offset = (int)(position % ChunkSize);

            ReadOnlySpan<TItem> available = FindChunk(chunkIndex, position).AsSpan(offset);
            int n = Math.Min(destination.Length, available.Length);

            available[..n].CopyTo(destination);
            destination = destination[n..];
            position += n;
        }
    }

    // Finds a chunk for a reader, or reports `index` as evicted. Call after reading the count.
    private TItem[] FindChunk(long chunkIndex, long index)
    {
        if (index < Volatile.Read(ref _firstRetained))
        {
            throw Evicted(index);
        }

        Chunk?[] slots = Volatile.Read(ref _slots);
        Chunk? chunk = slots[SlotOf(chunkIndex, slots.Length)];

        // Missing, or the slot already holds a newer chunk: freed after FirstRetained was read.
        return chunk is not null && chunk.Index == chunkIndex ? chunk.Values : throw Evicted(index);
    }

    private ArgumentOutOfRangeException Evicted(long index) => new(
        nameof(index),
        index,
        $"Value {index:N0} has been evicted; the oldest value in memory is {FirstRetained:N0}.");

    // ---- Writer side: one thread only, and only stores inside this library call these. ----

    /// <summary>How many values are written, published or not. Writer thread only.</summary>
    internal long Written => _written;

    /// <summary>Adds one value, not yet visible to readers.</summary>
    internal void Add(TItem item)
    {
        if (_writeChunk is null || _writeOffset == ChunkSize)
        {
            MoveToWriteChunk();
        }

        _writeChunk!.Values[_writeOffset++] = item;
        _written++;
    }

    /// <summary>Adds values in bulk, not yet visible to readers.</summary>
    internal void AddRange(ReadOnlySpan<TItem> items)
    {
        while (!items.IsEmpty)
        {
            if (_writeChunk is null || _writeOffset == ChunkSize)
            {
                MoveToWriteChunk();
            }

            Span<TItem> free = _writeChunk!.Values.AsSpan(_writeOffset);
            int n = Math.Min(items.Length, free.Length);

            items[..n].CopyTo(free);
            items = items[n..];
            _writeOffset += n;
            _written += n;
        }
    }

    /// <summary>
    /// Moves the write position forward by <paramref name="count"/> values without writing them
    /// (ADR 0006, point 7): for values that would be evicted at once. Their indices are used up, so
    /// numbering stays absolute, and no memory is allocated for them. The caller must evict them
    /// with <see cref="Retain"/> before the next <see cref="Publish"/>.
    /// </summary>
    internal void Skip(long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count == 0)
        {
            return;
        }

        _written += count;
        _writeChunk = null; // Found again from the ring on the next write.
    }

    /// <summary>
    /// Allocates chunks in advance, so that the next <paramref name="count"/> values (beyond what
    /// is already written) need no allocation.
    /// </summary>
    internal void Reserve(long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        long needed = (count + ChunkSize - 1) / ChunkSize;
        while (_spare.Count < needed)
        {
            _spare.Push(new Chunk(-1, AllocateValues()));
        }
    }

    // Lets a store reserve a column it only knows as an object, without knowing its element type.
    void IColumn.Reserve(long count) => Reserve(count);

    /// <summary>Makes every value written so far visible to readers.</summary>
    internal void Publish() => Volatile.Write(ref _count, _written);

    /// <summary>
    /// Evicts every value below <paramref name="first"/>: frees the chunks that hold nothing from
    /// <paramref name="first"/> on, and moves <see cref="FirstRetained"/> up. Never moves it back.
    /// Call before <see cref="Publish"/>, so readers never see skipped values.
    /// </summary>
    internal void Retain(long first)
    {
        first = Math.Min(first, _written);
        if (first <= _firstRetained)
        {
            return;
        }

        long firstChunk = first / ChunkSize;
        Chunk?[] slots = _slots;

        if (firstChunk - _firstLiveChunk <= slots.Length)
        {
            for (long c = _firstLiveChunk; c < firstChunk; c++)
            {
                Free(slots, SlotOf(c, slots.Length), c);
            }
        }
        else
        {
            // A long jump (after a big skip): cheaper to look at every slot once.
            for (int s = 0; s < slots.Length; s++)
            {
                if (slots[s] is { } chunk && chunk.Index < firstChunk)
                {
                    Free(slots, s, chunk.Index);
                }
            }
        }

        _firstLiveChunk = firstChunk;
        Volatile.Write(ref _firstRetained, first);
    }

    /// <summary>
    /// Returns a value that is written but maybe not yet published. Writer thread only; the value
    /// must still be in memory.
    /// </summary>
    internal TItem ReadWritten(long index)
    {
        long chunkIndex = index / ChunkSize;
        Chunk chunk = _slots[SlotOf(chunkIndex, _slots.Length)] is { } c && c.Index == chunkIndex
            ? c
            : throw new InvalidOperationException($"Value {index:N0} is not in memory.");
        return chunk.Values[index % ChunkSize];
    }

    /// <summary>
    /// Forgets values written since the last <see cref="Publish"/>, as if they were never added.
    /// Used when an append fails halfway, so the column stays in step with the others.
    /// </summary>
    internal void Rollback()
    {
        _written = _count;
        _writeChunk = null; // Found again from the ring on the next write.
    }

    // Points the write position at the chunk holding value `_written`, placing a chunk there if
    // there isn't one yet.
    private void MoveToWriteChunk()
    {
        long chunkIndex = _written / ChunkSize;
        _writeOffset = (int)(_written % ChunkSize);

        Chunk? chunk = _slots[SlotOf(chunkIndex, _slots.Length)];
        if (chunk is null || chunk.Index != chunkIndex)
        {
            // A reserved chunk if there is one, otherwise a new allocation. The index is set
            // before the chunk can be seen, and never changes afterwards.
            chunk = _spare.Count > 0 ? _spare.Pop() : new Chunk(-1, AllocateValues());
            chunk.Index = chunkIndex;
            Place(chunk);
        }

        _writeChunk = chunk;
    }

    private void Place(Chunk chunk)
    {
        while (true)
        {
            Chunk?[] slots = _slots;
            int slot = SlotOf(chunk.Index, slots.Length);
            if (slots[slot] is null)
            {
                slots[slot] = chunk;
                return;
            }

            Grow();
        }
    }

    // Doubles the ring until every live chunk has a slot of its own, then swaps it in. Readers
    // holding the old ring still find valid chunks in it.
    private void Grow()
    {
        Chunk?[] old = _slots;
        for (int length = old.Length * 2; ; length *= 2)
        {
            var bigger = new Chunk?[length];
            bool fits = true;

            foreach (Chunk? chunk in old)
            {
                if (chunk is null)
                {
                    continue;
                }

                int slot = SlotOf(chunk.Index, length);
                if (bigger[slot] is not null)
                {
                    fits = false;
                    break;
                }

                bigger[slot] = chunk;
            }

            if (fits)
            {
                Volatile.Write(ref _slots, bigger);
                return;
            }
        }
    }

    private void Free(Chunk?[] slots, int slot, long chunkIndex)
    {
        if (slots[slot] is { } chunk && chunk.Index == chunkIndex)
        {
            // Left to the garbage collector, not reused: a reader may still hold it (ADR 0006, point 9).
            slots[slot] = null;
            Volatile.Write(ref _chunksFreed, _chunksFreed + 1);
        }
    }

    private TItem[] AllocateValues()
    {
        Volatile.Write(ref _chunksAllocated, _chunksAllocated + 1);

        // "Uninitialized" skips zeroing memory that appends overwrite; readers never see it,
        // because they stay inside the published, retained range.
        return GC.AllocateUninitializedArray<TItem>(ChunkSize);
    }

    // The ring's length is always a power of two, so "mod length" is a bit mask.
    private static int SlotOf(long chunkIndex, int length) => (int)(chunkIndex & (length - 1));

    // One chunk of values, and which chunk of the column it is.
    private sealed class Chunk(long index, TItem[] values)
    {
        public long Index { get; set; } = index;

        public TItem[] Values { get; } = values;
    }
}

/// <summary>
/// A column seen without its element type, so a store can reserve and measure columns of any type.
/// </summary>
internal interface IColumn
{
    long ChunksAllocated { get; }

    long ChunksFreed { get; }

    void Reserve(long count);
}
