using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Laminar.Core;

/// <summary>
/// A group of records moving through the pipeline together, from a source to a store.
/// </summary>
/// <typeparam name="T">
/// The record type: an <see langword="unmanaged"/> struct (ADR 0005), so a batch's records are
/// plain memory with no references for the garbage collector to follow.
/// </typeparam>
/// <remarks>
/// <para>
/// <b>Two kinds of batch:</b>
/// </para>
/// <list type="bullet">
///   <item><b>Pooled</b> (the normal case): rented from a pipeline's <see cref="BatchPool{T}"/>
///   through <see cref="IBatchWriter{T}.RentBatch"/>. Its arrays are allocated once, in
///   <c>Prepare</c>, and <see cref="Dispose"/> hands the whole batch back to the pool for reuse.</item>
///   <item><b>Standalone</b>: created with the public constructor, for tools and benchmarks
///   outside a pipeline. It borrows its arrays from the shared <see cref="ArrayPool{T}"/> and
///   returns them in <see cref="Dispose"/>.</item>
/// </list>
/// <para>
/// <b>Ownership:</b> exactly one component owns a batch at a time, and the last owner disposes it.
/// </para>
/// <list type="table">
///   <listheader><term>Moment</term><description>Who disposes</description></listheader>
///   <item><term>Rented, being filled</term><description>The source, if it decides not to push.</description></item>
///   <item><term>Pushed to a writer</term><description>Ownership passes to the writer, even if the batch is dropped.</description></item>
///   <item><term>Dropped by the full-buffer policy</term><description>The buffer, immediately.</description></item>
///   <item><term>Taken by the ingestion worker</term><description>The worker, after the store has copied the records.</description></item>
/// </list>
/// <para>
/// Using a pooled batch after disposing it is a serious bug: it may already have been rented again
/// by another source. That can't always be detected.
/// </para>
/// <para>
/// <b>Usage:</b> the source writes records into <see cref="Buffer"/>, calls <see cref="SetCount"/>,
/// then pushes the batch. After that, everyone reads <see cref="Records"/> only.
/// </para>
/// <para>
/// <b>Payload (ADR 0004):</b> records with nested, variable-length data (such as a waveform per
/// event) keep that data in the batch's payload buffer. The source calls
/// <see cref="AllocatePayload"/> for each record's nested data and stores the returned
/// <see cref="PayloadSlice{TItem}"/> in the record. Stores read it back through
/// <see cref="Payload"/>.
/// </para>
/// <para>
/// This is a class, not a struct, on purpose: copies of a struct would share the same arrays, and
/// disposing two copies would return them twice.
/// </para>
/// </remarks>
public sealed class Batch<T> : IDisposable
    where T : unmanaged
{
    // The two states a batch can be in. An int, not a bool, so Interlocked can swap it.
    private const int InUse = 0;
    private const int Released = 1;

    // Payload items start on 8-byte boundaries, so any primitive type can be read efficiently.
    private const int PayloadAlignment = 8;

    // The smallest payload buffer created, so tiny first allocations don't grow it again at once.
    private const int MinimumPayloadBytes = 256;

    // The pool this batch belongs to, or null for a standalone batch.
    private readonly BatchPool<T>? _pool;

    // The record array. A standalone batch sets it to null when disposed.
    private T[]? _array;

    // The payload bytes. Null until first needed, unless sized up front.
    private byte[]? _payload;

    private int _state;

    /// <summary>
    /// Creates a standalone batch, outside any pipeline, with room for at least
    /// <paramref name="capacity"/> records.
    /// </summary>
    /// <remarks>
    /// Inside a pipeline, rent batches from the writer instead (<see cref="IBatchWriter{T}.RentBatch"/>),
    /// so they come from the pipeline's prepared pool.
    /// </remarks>
    /// <param name="capacity">
    /// The minimum number of records the batch must hold. The shared pool may supply a bigger
    /// array; see <see cref="Capacity"/>.
    /// </param>
    /// <param name="sequence">
    /// This batch's position in the source's stream: 0 for the first batch, increasing by 1 each
    /// time. Lets downstream code detect missing batches.
    /// </param>
    /// <param name="payloadBytes">
    /// How many payload bytes to reserve up front. 0 (the default) reserves nothing until
    /// <see cref="AllocatePayload"/> is first called. The buffer grows when needed either way.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="capacity"/> is zero or negative, or <paramref name="payloadBytes"/> is negative.
    /// </exception>
    public Batch(int capacity, long sequence, int payloadBytes = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadBytes);

        _array = ArrayPool<T>.Shared.Rent(capacity);
        if (payloadBytes > 0)
        {
            _payload = ArrayPool<byte>.Shared.Rent(payloadBytes);
        }

        Capacity = _array.Length;
        Sequence = sequence;
        CreatedTimestamp = Stopwatch.GetTimestamp();
    }

    // Creates a pooled batch. Its arrays are allocated here, once, and kept for its whole life.
    // It starts released: the pool hands it out with Reset.
    internal Batch(BatchPool<T> pool, int capacity, int payloadBytes)
    {
        _pool = pool;

        // "Uninitialized" skips zeroing memory that the source overwrites anyway.
        _array = GC.AllocateUninitializedArray<T>(capacity);
        if (payloadBytes > 0)
        {
            _payload = GC.AllocateUninitializedArray<byte>(payloadBytes);
        }

        Capacity = capacity;
        _state = Released;
    }

    /// <summary>
    /// This batch's position in the source's stream. A gap between consecutive batches
    /// (for example 39, then 41) means a batch was lost.
    /// </summary>
    public long Sequence { get; private set; }

    /// <summary>
    /// When the batch was created or rented, from <see cref="Stopwatch.GetTimestamp"/>.
    /// Used to measure latency through the pipeline.
    /// </summary>
    /// <remarks>
    /// Stopwatch ticks, not a wall-clock time: they never jump backwards when the system clock is
    /// adjusted. Convert an interval with <see cref="Stopwatch.GetElapsedTime(long)"/>.
    /// </remarks>
    public long CreatedTimestamp { get; private set; }

    /// <summary>
    /// How many records fit in the batch. For a standalone batch this may be more than was
    /// requested, because the shared pool hands out arrays in fixed sizes.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// How many records are valid: the first <see cref="Count"/> items of <see cref="Buffer"/>.
    /// Starts at 0; set with <see cref="SetCount"/>.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// The whole array, writable, for the source to fill before pushing the batch.
    /// </summary>
    /// <remarks>
    /// A fresh window over the array each time it is read; nothing is copied. Its length is
    /// <see cref="Capacity"/>. Items beyond <see cref="Count"/> may hold leftover data from a
    /// previous use of the array.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    public Span<T> Buffer => GetArray().AsSpan();

    /// <summary>
    /// The valid records, read-only: the first <see cref="Count"/> items of the array.
    /// </summary>
    /// <remarks>
    /// A fresh window over the array each time it is read; nothing is copied. Use it right away
    /// and don't keep it: the array is reused once the batch is disposed.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    public ReadOnlySpan<T> Records => GetArray().AsSpan(0, Count);

    /// <summary>
    /// Sets how many records at the start of <see cref="Buffer"/> are valid.
    /// </summary>
    /// <param name="count">Between 0 and <see cref="Capacity"/>, inclusive.</param>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is negative or greater than <see cref="Capacity"/>.
    /// </exception>
    public void SetCount(int count)
    {
        _ = GetArray(); // Throws if disposed.
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, Capacity);
        Count = count;
    }

    /// <summary>
    /// How many payload bytes are in use, including alignment padding between slices.
    /// </summary>
    public int PayloadBytesUsed { get; private set; }

    /// <summary>
    /// Reserves room for <paramref name="count"/> items of nested data in the payload buffer and
    /// returns where they are, for the record to keep.
    /// </summary>
    /// <typeparam name="TItem">The item type, such as <see cref="short"/> for waveform samples.</typeparam>
    /// <param name="count">How many items to reserve. 0 gives an empty slice.</param>
    /// <param name="items">
    /// The reserved items, writable, for the source to fill. <b>Fill them before the next call:</b>
    /// a later call may grow the buffer, moving it to a new array, and this span would then point
    /// at the old one. The slice stays valid either way, because it holds a position, not memory.
    /// </param>
    /// <returns>The slice to store in the record.</returns>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is negative, or the payload would exceed the largest possible array.
    /// </exception>
    public PayloadSlice<TItem> AllocatePayload<TItem>(int count, out Span<TItem> items)
        where TItem : unmanaged
    {
        _ = GetArray(); // Throws if disposed.
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        if (count == 0)
        {
            items = [];
            return default;
        }

        // Round the start up to the next multiple of 8. (x + 7) & ~7 does that with bit masks:
        // ~7 is all ones except the lowest three bits, so the & clears them.
        int start = (PayloadBytesUsed + PayloadAlignment - 1) & ~(PayloadAlignment - 1);
        long end = start + ((long)count * Unsafe.SizeOf<TItem>());
        ArgumentOutOfRangeException.ThrowIfGreaterThan(end, Array.MaxLength, nameof(count));

        byte[] payload = EnsurePayloadCapacity((int)end);
        PayloadBytesUsed = (int)end;

        // View the reserved bytes as TItems. Nothing is copied.
        items = MemoryMarshal.Cast<byte, TItem>(payload.AsSpan(start, (int)end - start));
        return new PayloadSlice<TItem>(start, count);
    }

    /// <summary>
    /// Returns the items a <see cref="PayloadSlice{TItem}"/> points at, read-only.
    /// </summary>
    /// <remarks>
    /// A view of the payload buffer; nothing is copied. Use it right away and don't keep it.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="slice"/> is outside the payload in use, so it can't have come from this batch.
    /// </exception>
    public ReadOnlySpan<TItem> GetPayload<TItem>(PayloadSlice<TItem> slice)
        where TItem : unmanaged
        => Payload.Get(slice);

    /// <summary>
    /// Returns the items a <see cref="PayloadSlice{TItem}"/> points at, writable, for the source to
    /// fill or change before pushing the batch.
    /// </summary>
    /// <remarks>
    /// Use this to get a fresh span after later <see cref="AllocatePayload"/> calls may have moved
    /// the buffer. The typical case is nested items that hold slices themselves: allocate the
    /// parent items, then each child's data, then write each parent item through this method.
    /// The same rule applies to the result: it is only valid until the next allocation.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="slice"/> is outside the payload in use, so it can't have come from this batch.
    /// </exception>
    public Span<TItem> GetWritablePayload<TItem>(PayloadSlice<TItem> slice)
        where TItem : unmanaged
    {
        ReadOnlySpan<TItem> items = Payload.Get(slice); // Checks disposal and bounds.

        // Same memory, without the read-only wrapper. Allowed here because the batch owns the
        // buffer and the source still owns the batch.
        return MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(items), items.Length);
    }

    /// <summary>
    /// Read access to the payload buffer, for code that resolves many slices (such as a layout
    /// store). Use it only while the batch is alive.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    public PayloadReader Payload
    {
        get
        {
            _ = GetArray(); // Throws if disposed.
            return new PayloadReader(_payload, PayloadBytesUsed);
        }
    }

    /// <summary>
    /// Releases the batch: a pooled batch goes back to its pool for reuse, a standalone batch
    /// returns its arrays to the shared pool. After this, every member throws
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <remarks>
    /// Safe to call more than once, even from several threads at the same time: only the first
    /// call releases anything. Arrays are never cleared, because unmanaged records hold no
    /// references for the garbage collector to free (ADR 0005).
    /// </remarks>
    public void Dispose()
    {
        // Swap the state to Released and get the old state in one atomic step, so exactly one
        // caller releases the batch.
        if (Interlocked.Exchange(ref _state, Released) == Released)
        {
            return;
        }

        if (_pool is not null)
        {
            _pool.Return(this);
            return;
        }

        T[]? array = _array;
        byte[]? payload = _payload;
        _array = null;
        _payload = null;

        ArrayPool<T>.Shared.Return(array!);
        if (payload is not null)
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    // Makes a pooled batch ready for its next use. Called by the pool just before handing it out.
    internal void Reset(long sequence)
    {
        Sequence = sequence;
        CreatedTimestamp = Stopwatch.GetTimestamp();
        Count = 0;
        PayloadBytesUsed = 0;
        Volatile.Write(ref _state, InUse);
    }

    // Makes sure the payload buffer holds at least `needed` bytes, growing it if necessary, and
    // returns it. Growing creates a bigger buffer and copies the bytes in use.
    private byte[] EnsurePayloadCapacity(int needed)
    {
        byte[]? current = _payload;
        if (current is not null && current.Length >= needed)
        {
            return current;
        }

        // Double, so a batch filled one record at a time grows only a few times.
        int doubled = (int)Math.Min((long)(current?.Length ?? 0) * 2, Array.MaxLength);
        int size = Math.Max(needed, Math.Max(doubled, MinimumPayloadBytes));

        byte[] bigger;
        if (_pool is not null)
        {
            // A pooled batch keeps its bigger buffer for good. Growing means the pool's payload
            // size was too small for this data, so it is counted (ADR 0005, point 5).
            bigger = GC.AllocateUninitializedArray<byte>(size);
            _pool.OnPayloadGrown();
        }
        else
        {
            bigger = ArrayPool<byte>.Shared.Rent(size);
        }

        if (current is not null)
        {
            current.AsSpan(0, PayloadBytesUsed).CopyTo(bigger);
            if (_pool is null)
            {
                ArrayPool<byte>.Shared.Return(current);
            }
        }

        _payload = bigger;
        return bigger;
    }

    // Returns the array, or throws if the batch is released.
    private T[] GetArray()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _state) == Released, this);
        return _array!;
    }
}
