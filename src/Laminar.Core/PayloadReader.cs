using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Laminar.Core;

/// <summary>
/// Read access to one batch's payload buffer: turns a <see cref="PayloadSlice{TItem}"/> back into
/// its items. Get one from <see cref="Batch{T}.Payload"/>.
/// </summary>
/// <remarks>
/// A small handle (an array reference and a length) passed by value to layout accessors. Use it
/// only while the batch is alive and don't keep it: the buffer goes back to the pool when the
/// batch is disposed.
/// </remarks>
public readonly struct PayloadReader
{
    private readonly byte[]? _buffer;
    private readonly int _length;

    internal PayloadReader(byte[]? buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    /// <summary>
    /// Returns the items <paramref name="slice"/> points at, read-only. Nothing is copied.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="slice"/> is outside the payload in use, so it can't have come from this batch.
    /// </exception>
    public ReadOnlySpan<TItem> Get<TItem>(PayloadSlice<TItem> slice)
        where TItem : unmanaged
    {
        if (slice.IsEmpty)
        {
            return [];
        }

        long end = slice.Offset + ((long)slice.Length * Unsafe.SizeOf<TItem>());
        if (slice.Offset < 0 || slice.Length < 0 || end > _length || _buffer is null)
        {
            throw new ArgumentOutOfRangeException(nameof(slice), slice, "The slice is outside this batch's payload.");
        }

        return MemoryMarshal.Cast<byte, TItem>(_buffer.AsSpan(slice.Offset, (int)end - slice.Offset));
    }
}
