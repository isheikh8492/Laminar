using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Laminar.Core;

/// <summary>
/// A group of records moving through the pipeline together, from a source to a store.
/// </summary>
/// <typeparam name="T">
/// The record type. Any type is allowed (ADR 0003); stores may require <c>unmanaged</c> structs.
/// </typeparam>
/// <remarks>
/// <para>
/// A batch borrows its array from <see cref="ArrayPool{T}.Shared"/> and returns it in
/// <see cref="Dispose"/>, so the pipeline creates almost no garbage after startup.
/// </para>
/// <para>
/// <b>Ownership:</b> exactly one component owns a batch at a time, and the last owner disposes it.
/// </para>
/// <list type="table">
///   <listheader><term>Moment</term><description>Who disposes</description></listheader>
///   <item><term>Created, being filled</term><description>The source, if it decides not to push.</description></item>
///   <item><term>Pushed to a writer</term><description>Ownership passes to the writer, even if the batch is dropped.</description></item>
///   <item><term>Dropped by the full-buffer policy</term><description>The buffer, immediately.</description></item>
///   <item><term>Taken by the ingestion worker</term><description>The worker, after the store has copied the records.</description></item>
/// </list>
/// <para>
/// <b>Usage:</b> the source writes records into <see cref="Buffer"/>, calls <see cref="SetCount"/>,
/// then pushes the batch. After that, everyone reads <see cref="Records"/> only. Writing to a batch
/// after pushing it is a bug.
/// </para>
/// <para>
/// <b>Payload (ADR 0004):</b> records with nested, variable-length data (such as a waveform per
/// event) keep that data in the batch's payload buffer, a second pooled array of bytes. The source
/// calls <see cref="AllocatePayload"/> for each record's nested data and stores the returned
/// <see cref="PayloadSlice{TItem}"/> in the record. Stores read it back with
/// <see cref="GetPayload"/>. The payload buffer is only rented when first used.
/// </para>
/// <para>
/// This is a class, not a struct, on purpose: copies of a struct would share the same pooled array,
/// and disposing two copies would return it to the pool twice.
/// </para>
/// </remarks>
public sealed class Batch<T> : IDisposable
{
    // The borrowed array. Null once the batch is disposed.
    private T[]? _array;

    // The borrowed payload bytes. Null until the first AllocatePayload, and again once disposed.
    private byte[]? _payload;

    // Payload items start on 8-byte boundaries, so any primitive type can be read efficiently.
    private const int PayloadAlignment = 8;

    // The smallest payload buffer rented, so tiny first allocations don't grow it again at once.
    private const int MinimumPayloadBytes = 256;

    /// <summary>
    /// Creates an empty batch with room for at least <paramref name="capacity"/> records.
    /// </summary>
    /// <param name="capacity">
    /// The minimum number of records the batch must hold. The pool may supply a bigger array;
    /// see <see cref="Capacity"/>.
    /// </param>
    /// <param name="sequence">
    /// This batch's position in the source's stream: 0 for the first batch, increasing by 1 each
    /// time. Lets downstream code detect missing batches.
    /// </param>
    /// <param name="payloadBytes">
    /// How many payload bytes to reserve up front, if the source knows roughly how much nested data
    /// the batch will carry. 0 (the default) rents nothing until <see cref="AllocatePayload"/> is
    /// first called. The buffer grows when needed either way.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="capacity"/> is zero or negative, or <paramref name="payloadBytes"/> is negative.
    /// </exception>
    public Batch(int capacity, long sequence, int payloadBytes = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadBytes);

        if (payloadBytes > 0)
        {
            _payload = ArrayPool<byte>.Shared.Rent(payloadBytes);
        }

        _array = ArrayPool<T>.Shared.Rent(capacity);
        Capacity = _array.Length;
        Sequence = sequence;
        CreatedTimestamp = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// This batch's position in the source's stream. A gap between consecutive batches
    /// (for example 39, then 41) means a batch was lost.
    /// </summary>
    public long Sequence { get; }

    /// <summary>
    /// When the batch was created, from <see cref="Stopwatch.GetTimestamp"/>.
    /// Used to measure latency through the pipeline.
    /// </summary>
    /// <remarks>
    /// Stopwatch ticks, not a wall-clock time: they never jump backwards when the system clock is
    /// adjusted. Convert an interval with <see cref="Stopwatch.GetElapsedTime(long)"/>.
    /// </remarks>
    public long CreatedTimestamp { get; }

    /// <summary>
    /// How many records fit in the batch: the length of the array actually received from the pool,
    /// which may be larger than the capacity requested.
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
    /// and don't keep it: the array goes back to the pool when the batch is disposed.
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
        ObjectDisposedException.ThrowIf(_array is null, this);
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
    /// A view of the payload buffer; nothing is copied. Use it right away and don't keep it: the
    /// buffer goes back to the pool when the batch is disposed.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The batch has been disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="slice"/> is outside the payload in use, so it can't have come from this batch.
    /// </exception>
    public ReadOnlySpan<TItem> GetPayload<TItem>(PayloadSlice<TItem> slice)
        where TItem : unmanaged
    {
        _ = GetArray(); // Throws if disposed.

        if (slice.IsEmpty)
        {
            return [];
        }

        long end = slice.Offset + ((long)slice.Length * Unsafe.SizeOf<TItem>());
        if (slice.Offset < 0 || slice.Length < 0 || end > PayloadBytesUsed || _payload is null)
        {
            throw new ArgumentOutOfRangeException(nameof(slice), slice, "The slice is outside this batch's payload.");
        }

        return MemoryMarshal.Cast<byte, TItem>(_payload.AsSpan(slice.Offset, (int)end - slice.Offset));
    }

    /// <summary>
    /// Returns the arrays to the pool. After this, <see cref="Buffer"/>, <see cref="Records"/> and
    /// <see cref="SetCount"/> throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <remarks>
    /// Safe to call more than once, even from several threads at the same time: only the first
    /// call returns the array, so it can never go back to the pool twice. The array is cleared on
    /// return only when <typeparamref name="T"/> contains references, so the garbage collector can
    /// free the objects they point to; plain structs skip that cost.
    /// </remarks>
    public void Dispose()
    {
        // Swap the field to null and take the old value in one atomic step, so exactly one
        // caller gets the array.
        T[]? array = Interlocked.Exchange(ref _array, null);
        if (array is null)
        {
            return;
        }

        ArrayPool<T>.Shared.Return(array, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());

        // Only the caller that won the exchange above gets here, so this runs once. Payload bytes
        // hold no references, so they never need clearing.
        byte[]? payload = _payload;
        _payload = null;
        if (payload is not null)
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    // Makes sure the payload buffer holds at least `needed` bytes, growing it if necessary, and
    // returns it. Growing rents a bigger array, copies the bytes in use, and returns the old one.
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
        byte[] bigger = ArrayPool<byte>.Shared.Rent(size);

        if (current is not null)
        {
            current.AsSpan(0, PayloadBytesUsed).CopyTo(bigger);
            ArrayPool<byte>.Shared.Return(current);
        }

        _payload = bigger;
        return bigger;
    }

    // Returns the array, or throws if the batch is disposed. Reads the field once into a local,
    // so it can't become null between the check and the use.
    private T[] GetArray()
    {
        T[]? array = _array;
        ObjectDisposedException.ThrowIf(array is null, this);
        return array;
    }
}
