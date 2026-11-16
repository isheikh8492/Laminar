namespace Laminar.Core;

/// <summary>
/// Where a record's nested data sits in its batch's payload buffer (ADR 0004): for example, the
/// samples of one event's waveform.
/// </summary>
/// <typeparam name="TItem">
/// The type of the items, such as <see cref="short"/> for waveform samples. It is part of the
/// slice's type, so a slice of <see cref="short"/> can't be read back as <see cref="float"/> by
/// mistake.
/// </typeparam>
/// <remarks>
/// <para>
/// A record holds a slice instead of an array, so it stays fixed-size and <c>unmanaged</c>, and no
/// memory is allocated per record. Get one from <see cref="Batch{T}.AllocatePayload"/> and read
/// the items back with <see cref="Batch{T}.GetPayload"/>.
/// </para>
/// <para>
/// A slice only means something together with the batch it came from. Once that batch is
/// disposed, the slice points at nothing.
/// </para>
/// </remarks>
/// <param name="Offset">Where the items start, in bytes from the start of the payload buffer.</param>
/// <param name="Length">How many items there are (not bytes).</param>
public readonly record struct PayloadSlice<TItem>(int Offset, int Length)
    where TItem : unmanaged
{
    /// <summary>Whether the slice holds no items. The default slice is empty.</summary>
    public bool IsEmpty => Length == 0;
}
