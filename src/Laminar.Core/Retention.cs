namespace Laminar.Core;

/// <summary>The kinds of <see cref="Retention"/>.</summary>
public enum RetentionKind
{
    /// <summary>Everything stays in memory for the whole run.</summary>
    KeepAll,

    /// <summary>At least the latest <see cref="Retention.Items"/> items stay in memory.</summary>
    Latest,

    /// <summary>A nested table keeps the items of every parent item its parent still holds.</summary>
    SameAsParent,
}

/// <summary>
/// How much of one table stays in memory (ADR 0006). Retention decides only what stays readable;
/// what goes to disk is the recorder's business, and recorded data is never freed before it is saved.
/// </summary>
/// <remarks>
/// For a nested table, <see cref="Items"/> counts <b>parent</b> items: <c>Latest(5)</c> on
/// waveforms keeps the waveforms of the latest 5 events, however many samples that is. A nested
/// table never keeps more than its parent.
/// </remarks>
public readonly record struct Retention
{
    private Retention(RetentionKind kind, long items, long max)
    {
        Kind = kind;
        Items = items;
        Max = max;
    }

    /// <summary>Everything stays in memory. The default.</summary>
    public static Retention KeepAll => default;

    /// <summary>A nested table follows its parent: it keeps the items of every parent item still in memory.</summary>
    public static Retention SameAsParent => new(RetentionKind.SameAsParent, 0, 0);

    /// <summary>Which kind of retention this is.</summary>
    public RetentionKind Kind { get; }

    /// <summary>
    /// For <see cref="RetentionKind.Latest"/>: how many items to keep. For a nested table, how many
    /// parent items' worth. Zero for the other kinds.
    /// </summary>
    public long Items { get; }

    /// <summary>
    /// For <see cref="RetentionKind.Latest"/>: the largest window the run is prepared for. Growing
    /// the window up to it allocates nothing; beyond it is allowed, and the allocations are counted.
    /// </summary>
    public long Max { get; }

    /// <summary>
    /// Keeps at least the latest <paramref name="items"/> items in memory, prepared for windows up to
    /// <paramref name="max"/>.
    /// </summary>
    /// <param name="items">How many items to keep. For a nested table, how many parent items' worth.</param>
    /// <param name="max">The largest window to prepare for; at least <paramref name="items"/>. Defaults to <paramref name="items"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="items"/> is negative, or <paramref name="max"/> is less than <paramref name="items"/>.
    /// </exception>
    public static Retention Latest(long items, long? max = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(items);
        long limit = max ?? items;
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, items, nameof(max));
        return new Retention(RetentionKind.Latest, items, limit);
    }

    /// <inheritdoc/>
    public override string ToString() => Kind switch
    {
        RetentionKind.Latest => $"latest {Items:N0} (max {Max:N0})",
        RetentionKind.SameAsParent => "same as parent",
        _ => "keep all",
    };
}
