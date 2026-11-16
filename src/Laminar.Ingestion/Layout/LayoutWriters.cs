using Laminar.Core;

namespace Laminar.Ingestion.Layout;

// The code half of a layout (ADR 0004, point 7): the objects a LayoutStore uses to split records
// into tables. A RecordLayout holds factories for these; each LayoutStore creates its own set, so
// one layout can serve many stores.
//
// Everything here runs on the single writer thread and is internal: users only see the builder
// and the store.

/// <summary>Where a store's columns are registered by name, so readers can find them.</summary>
internal sealed class LayoutColumns(int chunkSize)
{
    public int ChunkSize { get; } = chunkSize;

    public Dictionary<(string Table, string Field), object> Fields { get; } = [];

    public Dictionary<string, object> Rows { get; } = [];

    public Dictionary<string, ChunkedColumn<long>> Offsets { get; } = [];
}

/// <summary>
/// One part of a table: a field column, the table's rows, or a nested table. Works on a whole
/// span of items at a time, so each part streams into its own memory, one part after another.
/// </summary>
internal interface IPartWriter<TOwner>
{
    void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload);

    void Publish();

    void Rollback();
}

/// <summary>A table's description, able to create a fresh writer for each store.</summary>
internal sealed class TableDefinition<TOwner>(Func<LayoutColumns, IPartWriter<TOwner>>[] parts)
{
    public TableWriter<TOwner> CreateWriter(LayoutColumns columns)
    {
        var writers = new IPartWriter<TOwner>[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            writers[i] = parts[i](columns);
        }

        return new TableWriter<TOwner>(writers);
    }
}

/// <summary>Writes items into every part of one table and keeps the table's own count.</summary>
internal sealed class TableWriter<TOwner>(IPartWriter<TOwner>[] parts)
{
    private long _published;

    /// <summary>Items written to this table, published or not.</summary>
    public long Written { get; private set; }

    public void WriteRange(ReadOnlySpan<TOwner> items, PayloadReader payload)
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.WriteRange(items, payload);
        }

        Written += items.Length;
    }

    public void Publish()
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Publish();
        }

        _published = Written;
    }

    public void Rollback()
    {
        foreach (IPartWriter<TOwner> part in parts)
        {
            part.Rollback();
        }

        Written = _published;
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

    public void Publish() => column.Publish();

    public void Rollback() => column.Rollback();
}

/// <summary>The rows of a Rows table: whole items, copied in bulk with no accessor calls.</summary>
internal sealed class RowWriter<TItem>(ChunkedColumn<TItem> column) : IPartWriter<TItem>
    where TItem : unmanaged
{
    public void WriteRange(ReadOnlySpan<TItem> items, PayloadReader payload) => column.AddRange(items);

    public void Publish() => column.Publish();

    public void Rollback() => column.Rollback();
}

/// <summary>
/// A nested table: for each owner item, resolves its payload slice, writes the nested items into
/// the child table, and records where they end in the offsets column.
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
        foreach (TOwner item in items)
        {
            ReadOnlySpan<TItem> children = payload.Get(_select(item));
            _child.WriteRange(children, payload);
            _offsets.Add(_child.Written);
        }
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
}
