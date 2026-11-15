using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

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
/// This is a class, not a struct, on purpose: copies of a struct would share the same pooled array,
/// and disposing two copies would return it to the pool twice.
/// </para>
/// </remarks>
public sealed class Batch<T> : IDisposable
{
    // The borrowed array. Null once the batch is disposed.
    private T[]? _array;

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
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is zero or negative.</exception>
    public Batch(int capacity, long sequence)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

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
    /// Returns the array to the pool. After this, <see cref="Buffer"/>, <see cref="Records"/> and
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
