using Laminar.Core;

namespace Laminar.Ingestion.Layout;

/// <summary>
/// Describes one table of a layout: its fields and its nested tables (ADR 0004). You get one from
/// <see cref="RecordLayout.Columns{T}"/>, <see cref="RecordLayout.Rows{T}"/> or
/// <see cref="Many{TItem}"/>; you don't create it yourself.
/// </summary>
/// <typeparam name="TOwner">The type of one item in this table.</typeparam>
/// <remarks>
/// Every method returns the same builder, so calls can be chained:
/// <c>t.Field("fsc", p =&gt; p.Fsc).Field("ssc", p =&gt; p.Ssc)</c>. Use a normal block with loops
/// and conditions when the fields are only known at runtime.
/// </remarks>
public sealed class TableBuilder<TOwner>
{
    private readonly LayoutContext _context;
    private readonly int _index;
    private readonly string _name;
    private readonly TableStorage _storage;
    private readonly string? _parent;
    private readonly List<FieldSchema> _fields = [];
    private readonly HashSet<string> _fieldNames = [];
    private readonly List<Func<LayoutColumns, IPartWriter<TOwner>>> _parts = [];
    private bool _hasChildren;

    internal TableBuilder(
        LayoutContext context,
        string name,
        TableStorage storage,
        string? parent,
        Func<LayoutColumns, IPartWriter<TOwner>>? rowsPart)
    {
        _context = context;
        _index = context.Reserve(name);
        _name = name;
        _storage = storage;
        _parent = parent;

        if (rowsPart is not null)
        {
            _parts.Add(rowsPart);
        }
    }

    /// <summary>
    /// Adds a column holding one value per item, read from the item alone.
    /// </summary>
    /// <param name="name">The column's name, unique within this table.</param>
    /// <param name="get">Reads the value from an item, for example <c>p =&gt; p.Fsc</c>.</param>
    /// <exception cref="InvalidOperationException">This is a <see cref="TableStorage.Rows"/> table.</exception>
    /// <exception cref="ArgumentException">The name is empty or already used in this table.</exception>
    public TableBuilder<TOwner> Field<TValue>(string name, Func<TOwner, TValue> get)
        where TValue : unmanaged
    {
        ArgumentNullException.ThrowIfNull(get);
        return Field(name, (item, _) => get(item));
    }

    /// <summary>
    /// Adds a column holding one value per item, read from the item and its batch's payload. Use
    /// this for values stored in a payload slice, such as parameter <c>i</c> of a panel whose
    /// detectors are only known at runtime.
    /// </summary>
    /// <param name="name">The column's name, unique within this table.</param>
    /// <param name="get">
    /// Reads the value, for example <c>(p, payload) =&gt; payload.Get(p.Parameters)[i]</c>.
    /// </param>
    /// <exception cref="InvalidOperationException">This is a <see cref="TableStorage.Rows"/> table.</exception>
    /// <exception cref="ArgumentException">The name is empty or already used in this table.</exception>
    public TableBuilder<TOwner> Field<TValue>(string name, Func<TOwner, PayloadReader, TValue> get)
        where TValue : unmanaged
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(get);

        if (_storage == TableStorage.Rows)
        {
            throw new InvalidOperationException(
                $"Table '{_name}' stores whole rows, so it has no fields. Use TableStorage.Columns to choose fields.");
        }

        if (!_fieldNames.Add(name))
        {
            throw new ArgumentException($"Table '{_name}' already has a field called '{name}'.", nameof(name));
        }

        _fields.Add(new FieldSchema(name, typeof(TValue)));

        string table = _name;
        _parts.Add(columns =>
        {
            var column = new ChunkedColumn<TValue>(columns.ChunkSize);
            columns.Fields[(table, name)] = column;
            return new FieldWriter<TOwner, TValue>(get, column);
        });

        return this;
    }

    /// <summary>
    /// Adds a nested table: a variable number of items per item of this table, such as the samples
    /// of each event's waveform. The nested items are stored flattened, with offsets (ADR 0004).
    /// </summary>
    /// <typeparam name="TItem">The nested item type, such as <see cref="short"/> for samples.</typeparam>
    /// <param name="name">The nested table's name, unique within the whole layout.</param>
    /// <param name="select">Picks the item's payload slice, for example <c>p =&gt; p.Waveform</c>.</param>
    /// <param name="storage">
    /// How the nested table stores its items. <see cref="TableStorage.Rows"/> stores them whole and
    /// needs no <paramref name="configure"/>; <see cref="TableStorage.Columns"/> needs
    /// <paramref name="configure"/> to choose fields.
    /// </param>
    /// <param name="configure">
    /// Describes the nested table: its fields, and further nested tables to any depth.
    /// </param>
    /// <exception cref="ArgumentException">The name is empty or already used in this layout.</exception>
    public TableBuilder<TOwner> Many<TItem>(
        string name,
        Func<TOwner, PayloadSlice<TItem>> select,
        TableStorage storage,
        Action<TableBuilder<TItem>>? configure = null)
        where TItem : unmanaged
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(select);

        var child = new TableBuilder<TItem>(_context, name, storage, _name, RecordLayout.RowsPart<TItem>(name, storage));
        configure?.Invoke(child);
        TableDefinition<TItem> definition = child.Complete();

        _parts.Add(columns =>
        {
            var offsets = new ChunkedColumn<long>(columns.ChunkSize);
            columns.Offsets[name] = offsets;
            return new ManyWriter<TOwner, TItem>(select, definition.CreateWriter(columns), offsets);
        });

        _hasChildren = true;
        return this;
    }

    // Finishes the table: checks it, records its schema, and returns its definition.
    internal TableDefinition<TOwner> Complete()
    {
        if (_storage == TableStorage.Columns && _fields.Count == 0 && !_hasChildren)
        {
            throw new InvalidOperationException(
                $"Table '{_name}' stores columns but has no fields. Add at least one Field or Many.");
        }

        _context.Set(_index, new TableSchema(_name, _storage, typeof(TOwner), _parent, _fields.ToArray()));
        return new TableDefinition<TOwner>([.. _parts]);
    }
}

/// <summary>Collects the tables of one layout while it is being built.</summary>
internal sealed class LayoutContext
{
    private readonly List<TableSchema?> _tables = [];
    private readonly HashSet<string> _names = [];

    // Claims a table name and a position, so the root table stays first even though nested tables
    // finish before it.
    public int Reserve(string name)
    {
        if (!_names.Add(name))
        {
            throw new ArgumentException($"The layout already has a table called '{name}'.", nameof(name));
        }

        _tables.Add(null);
        return _tables.Count - 1;
    }

    public void Set(int index, TableSchema table) => _tables[index] = table;

    public LayoutSchema ToSchema()
    {
        var tables = new TableSchema[_tables.Count];
        for (int i = 0; i < tables.Length; i++)
        {
            tables[i] = _tables[i] ?? throw new InvalidOperationException("A table was never completed.");
        }

        return new LayoutSchema(tables);
    }
}
