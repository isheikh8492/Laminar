using Laminar.Core;

namespace Laminar.Ingestion.Layout;

// The code half of a layout (ADR 0004, point 7): the objects a LayoutStore uses to split records
// into tables. A RecordLayout holds factories for these; each LayoutStore creates its own set, so
// one layout can serve many stores.
//
// Everything here runs on the single writer thread and is internal, except where noted: users
// only see the builder and the store.

/// <summary>Where a store's columns and tables are registered by name, so readers can find them.</summary>
internal sealed class LayoutColumns(int chunkSize)
{
    public int ChunkSize { get; } = chunkSize;

    public Dictionary<(string Table, string Field), object> Fields { get; } = [];

    public Dictionary<string, object> Rows { get; } = [];

    public Dictionary<string, ChunkedColumn<long>> Offsets { get; } = [];

    public Dictionary<string, ITableState> Tables { get; } = [];

    // Every column belonging to each table: its fields or rows, and its offsets if it is nested.
    public Dictionary<string, List<IColumn>> ColumnsOfTable { get; } = [];

    public void Register(string table, IColumn column)
    {
        if (!ColumnsOfTable.TryGetValue(table, out List<IColumn>? list))
        {
            list = [];
            ColumnsOfTable[table] = list;
        }

        list.Add(column);
    }
}

/// <summary>
/// One table's retention while a store runs (ADR 0006, point 8). Any thread may request a new
/// one; the writer picks it up between batches, so the store keeps a single writer and the hot
/// path takes no locks.
/// </summary>
internal sealed class RetentionState(Retention initial)
{
    // Retention is a struct of three values, too big to read or write in one step, so each value
    // travels in its own small object, swapped whole. Only a request allocates, never an append.
    private Box _current = new(initial);
    private Box? _requested;

    /// <summary>The retention the writer is applying. Writer thread only.</summary>
    public Retention Current { get; private set; } = initial;

    /// <summary>The retention most recently asked for, applied or not. Any thread.</summary>
    public Retention Latest => (Volatile.Read(ref _requested) ?? Volatile.Read(ref _current)).Value;

    public void Request(Retention retention) => Volatile.Write(ref _requested, new Box(retention));

    /// <summary>Takes up a pending request, if any. Writer thread only.</summary>
    public void ApplyRequest()
    {
        Box? request = Interlocked.Exchange(ref _requested, null);
        if (request is not null)
        {
            Current = request.Value;
            Volatile.Write(ref _current, request);
        }
    }

    private sealed class Box(Retention value)
    {
        public Retention Value { get; } = value;
    }
}

/// <summary>What a store reports about one table, without knowing its item type.</summary>
internal interface ITableState
{
    RetentionState Retention { get; }

    long PublishedCount { get; }

    long PublishedFirstRetained { get; }

    long PublishedEvictedOnArrival { get; }
}

/// <summary>
/// One part of a table: a field column, the table's rows, or a nested table. Works on a whole
/// span of items at a time, so each part streams into its own memory, one part after another.
/// </summary>
internal interface IPartWriter<TOwner>
{
    void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload);

    // Uses up the items' indices without storing them, for items that would be evicted at once
    // (ADR 0006, point 7). Nested tables still record offsets, so numbering stays absolute.
    void SkipRange(ReadOnlySpan<TOwner> items, PayloadReader payload);

    // Evicts owner items below `firstOwner`; `ownerWritten` is how many owners exist now. Called
    // after writing and before publishing.
    void Retain(long firstOwner, long ownerWritten);

    void Publish();

    void Rollback();

    // Makes room for `items` owner items in advance (ADR 0005). Nested tables reserve only their
    // offsets: how many nested items each owner has isn't known until data arrives.
    void Reserve(long items);
}

/// <summary>A table's description, able to create a fresh writer for each store.</summary>
internal sealed class TableDefinition<TOwner>(string name, Retention retention, Func<LayoutColumns, IPartWriter<TOwner>>[] parts)
{
    public TableWriter<TOwner> CreateWriter(LayoutColumns columns)
    {
        var writers = new IPartWriter<TOwner>[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            writers[i] = parts[i](columns);
        }

        var writer = new TableWriter<TOwner>(writers, new RetentionState(retention));
        columns.Tables[name] = writer;
        return writer;
    }
}

/// <summary>Writes items into every part of one table and keeps the table's own counts.</summary>
internal sealed class TableWriter<TOwner>(IPartWriter<TOwner>[] parts, RetentionState retention) : ITableState
{
    private long _published;
    private long _publishedFirst;
    private long _skipped;
    private long _publishedSkipped;

    public RetentionState Retention => retention;

    /// <summary>Items written to this table, published or not, including skipped ones.</summary>
    public long Written { get; private set; }

    /// <summary>The oldest item still in memory, as the writer sees it (maybe not yet published).</summary>
    public long FirstRetained { get; private set; }

    public long PublishedCount => Volatile.Read(ref _published);

    public long PublishedFirstRetained => Volatile.Read(ref _publishedFirst);

    public long PublishedEvictedOnArrival => Volatile.Read(ref _publishedSkipped);

    public void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.WriteRange(items, payload);
        }

        Written += items.Length;
    }

    public void SkipRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.SkipRange(items, payload);
        }

        Written += items.Length;
        _skipped += items.Length;
    }

    /// <summary>Evicts items below <paramref name="first"/> in every part. Never moves back.</summary>
    public void Retain(long first)
    {
        first = Math.Clamp(first, FirstRetained, Written);
        FirstRetained = first;

        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Retain(first, Written);
        }
    }

    public void Publish()
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Publish();
        }

        Volatile.Write(ref _publishedFirst, FirstRetained);
        Volatile.Write(ref _publishedSkipped, _skipped);
        Volatile.Write(ref _published, Written);
    }

    public void Rollback()
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Rollback();
        }

        Written = _published;
        _skipped = _publishedSkipped;
    }

    public void Reserve(long items)
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Reserve(items);
        }
    }
}

/// <summary>One column of a Columns table: calls the accessor once per item.</summary>
internal sealed class FieldWriter<TOwner, TValue>(Func<TOwner, PayloadReader, TValue> get, ChunkedColumn<TValue> column)
    : IPartWriter<TOwner>
    where TValue : unmanaged
{
    public void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        foreach (TOwner item in items)
        {
            column.Add(get(item, payload));
        }
    }

    public void SkipRange(ReadOnlySpan<TOwner> items, PayloadReader payload) => column.Skip(items.Length);

    public void Retain(long firstOwner, long ownerWritten) => column.Retain(firstOwner);

    public void Publish() => column.Publish();

    public void Rollback() => column.Rollback();

    public void Reserve(long items) => column.Reserve(items);
}

/// <summary>The rows of a Rows table: whole items, copied in bulk with no accessor calls.</summary>
internal sealed class RowWriter<TItem>(ChunkedColumn<TItem> column) : IPartWriter<TItem>
    where TItem : unmanaged
{
    public void WriteRange(ReadOnlySpan<TItem> items, PayloadReader payload) => column.AddRange(items);

    public void SkipRange(ReadOnlySpan<TItem> items, PayloadReader payload) => column.Skip(items.Length);

    public void Retain(long firstOwner, long ownerWritten) => column.Retain(firstOwner);

    public void Publish() => column.Publish();

    public void Rollback() => column.Rollback();

    public void Reserve(long items) => column.Reserve(items);
}

/// <summary>
/// A nested table: for each owner item, resolves its payload slice, writes the nested items into
/// the child table, and records where they end in the offsets column. It also applies the child
/// table's retention, which counts owner items (ADR 0006, point 1).
/// </summary>
internal sealed class ManyWriter<TOwner, TItem> : IPartWriter<TOwner>
    where TItem : unmanaged
{
    private readonly Func<TOwner, PayloadSlice<TItem>> _select;
    private readonly TableWriter<TItem> _child;
    private readonly ChunkedColumn<long> _offsets;

    public ManyWriter(Func<TOwner, PayloadSlice<TItem>> select, TableWriter<TItem> child, ChunkedColumn<long> offsets)
    {
        _select = select;
        _child = child;
        _offsets = offsets;

        // offsets[i] is where owner i's items start, so the array always begins with 0 and has
        // one more entry than there are owners.
        _offsets.Add(0);
        _offsets.Publish();
    }

    public void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        // A window smaller than this range keeps only its last owners' items. The others would be
        // evicted the moment they were written, so they are skipped instead (ADR 0006, point 7).
        Retention retention = _child.Retention.Current;
        long keepFrom = retention.Kind == RetentionKind.Latest ? Math.Max(0, items.Length - retention.Items) : 0;

        for (int i = 0; i < items.Length; i++)
        {
            ReadOnlySpan<TItem> children = payload.Get(_select(items[i]));
            if (i < keepFrom)
            {
                _child.SkipRange(children, payload);
            }
            else
            {
                _child.WriteRange(children, payload);
            }

            _offsets.Add(_child.Written);
        }
    }

    public void SkipRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        // Offsets are still written: they are small, and the first kept owner's start is the
        // last skipped owner's end.
        foreach (TOwner item in items)
        {
            _child.SkipRange(payload.Get(_select(item)), payload);
            _offsets.Add(_child.Written);
        }
    }

    public void Retain(long firstOwner, long ownerWritten)
    {
        // Offsets follow the owners, so an owner still in memory can always say where its items
        // were, even when they have been evicted.
        _offsets.Retain(firstOwner);

        // The child never keeps more than its owner (ADR 0006, point 3): KeepAll and SameAsParent
        // follow the owner; Latest keeps the latest owners' worth, clamped to the owner's window.
        Retention retention = _child.Retention.Current;
        long firstKeptOwner = retention.Kind == RetentionKind.Latest
            ? Math.Max(firstOwner, ownerWritten - retention.Items)
            : firstOwner;

        _child.Retain(_offsets.ReadWritten(firstKeptOwner));
    }

    // Child first, offsets last: once a reader sees an offset, the items it points to are there.
    public void Publish()
    {
        _child.Publish();
        _offsets.Publish();
    }

    public void Rollback()
    {
        _child.Rollback();
        _offsets.Rollback();
    }

    // One offset per owner, plus the leading 0.
    public void Reserve(long items) => _offsets.Reserve(items + 1);
}
