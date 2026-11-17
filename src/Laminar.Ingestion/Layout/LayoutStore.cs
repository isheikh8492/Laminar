using Laminar.Core;

namespace Laminar.Ingestion.Layout;

/// <summary>
/// What one table of a <see cref="LayoutStore{T}"/> holds right now (ADR 0006).
/// </summary>
/// <param name="Table">The table's name.</param>
/// <param name="Retention">The retention being applied, or most recently asked for.</param>
/// <param name="Count">How many items the table has ever had. Indices run from 0 to this.</param>
/// <param name="FirstRetained">The oldest item still in memory.</param>
/// <param name="EvictedOnArrival">
/// Items never stored because they would have been evicted at once (ADR 0006, point 7). Included
/// in <see cref="Evicted"/>.
/// </param>
/// <param name="ChunksAllocated">Chunks the table's columns have allocated, including reserved ones.</param>
/// <param name="ChunksFreed">Chunks the table's columns have freed.</param>
public readonly record struct TableMetrics(
    string Table,
    Retention Retention,
    long Count,
    long FirstRetained,
    long EvictedOnArrival,
    long ChunksAllocated,
    long ChunksFreed)
{
    /// <summary>Items readable now: from <see cref="FirstRetained"/> up to <see cref="Count"/>.</summary>
    public long InMemory => Count - FirstRetained;

    /// <summary>Items no longer in memory: everything below <see cref="FirstRetained"/>.</summary>
    public long Evicted => FirstRetained;
}

/// <summary>
/// A store that splits each record into tables, following a <see cref="RecordLayout{T}"/>:
/// some parts as columns, some as rows, nested data with offsets (ADR 0004).
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>One append fills every table.</b> All tables are written, then published, then
/// <see cref="Count"/> is published last. A reader that reads <see cref="Count"/> first and stays
/// below it always finds every table complete for those records. Columns and offsets also have
/// their own <c>Count</c>, which may run slightly ahead of this one during an append.
/// </para>
/// <para>
/// <b>Retention (ADR 0006):</b> each table keeps everything, or a window of its latest items, as
/// declared in the layout and changeable with <see cref="SetRetention"/> while the run continues.
/// Indices stay absolute; <see cref="GetFirstRetained"/> says where each table's window starts,
/// and reading below it reports the item as evicted. Nothing is recorded to disk yet, so a chunk
/// is freed as soon as it leaves its window.
/// </para>
/// <para>
/// <b>Failure:</b> if an accessor throws partway through a batch, everything written for that
/// batch is rolled back before the exception continues, so the tables stay in step.
/// </para>
/// <para>
/// <b>Threading:</b> one writer (the ingestion worker) calls <see cref="Append"/>. Any number of
/// readers may use <see cref="Count"/>, the columns, the metrics and <see cref="SetRetention"/> at
/// the same time, without locks.
/// </para>
/// </remarks>
public sealed class LayoutStore<T> : IRecordStore<T>
    where T : unmanaged
{
    private readonly TableWriter<T> _root;
    private readonly LayoutColumns _columns;
    private long _count;

    /// <summary>
    /// Creates an empty store with the tables <paramref name="layout"/> describes.
    /// </summary>
    /// <param name="layout">Where each part of a record goes.</param>
    /// <param name="chunkSize">How many values each chunk of each column holds.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is zero or negative.</exception>
    public LayoutStore(RecordLayout<T> layout, int chunkSize = ChunkedColumn<long>.DefaultChunkSize)
    {
        ArgumentNullException.ThrowIfNull(layout);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);

        Schema = layout.Schema;
        _columns = new LayoutColumns(chunkSize);
        _root = layout.Root.CreateWriter(_columns);
    }

    /// <summary>Every table, its storage, fields, nesting and declared retention.</summary>
    public LayoutSchema Schema { get; }

    /// <inheritdoc/>
    /// <remarks>The number of records ever appended, which is the number of items in the root table.</remarks>
    public long Count => Volatile.Read(ref _count);

    /// <inheritdoc/>
    /// <remarks>
    /// Reserves every column of the root table, and the offsets of each nested table: for
    /// <paramref name="records"/> records, or for the root table's largest window if that is
    /// smaller. Nested items aren't reserved here, because how many each record has isn't known;
    /// reserve those with <see cref="ReserveNested"/>. Must be called from the writer's thread, or
    /// before appending starts.
    /// </remarks>
    public void Reserve(long records)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(records);

        // A window never needs more than its largest size, plus the one chunk it may run over by.
        Retention retention = _root.Retention.Current;
        if (retention.Kind == RetentionKind.Latest)
        {
            records = Math.Min(records, retention.Max + _columns.ChunkSize);
        }

        _root.Reserve(records);
    }

    /// <summary>
    /// Reserves room for <paramref name="items"/> items in one nested table, for example the total
    /// number of waveform samples expected: records × average samples per record. For a table with
    /// a window, reserve its largest window's worth.
    /// </summary>
    /// <param name="table">The nested table's name.</param>
    /// <param name="items">How many items to make room for.</param>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    /// <remarks>
    /// Reserves the table's own rows or columns. Tables nested inside it are not reserved; call
    /// this for each of them too. Must be called from the writer's thread, or before appending starts.
    /// </remarks>
    public void ReserveNested(string table, long items)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(items);
        TableSchema schema = FindTable(table);
        if (schema.Parent is null)
        {
            throw new KeyNotFoundException($"'{table}' is the root table; reserve it with Reserve.");
        }

        if (_columns.Rows.TryGetValue(table, out object? rows))
        {
            ((IColumn)rows).Reserve(items);
        }

        foreach (FieldSchema field in schema.Fields)
        {
            ((IColumn)_columns.Fields[(table, field.Name)]).Reserve(items);
        }
    }

    /// <inheritdoc/>
    /// <remarks>Must only be called from one thread at a time.</remarks>
    public void Append(Batch<T> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        // Retention changes asked for since the last batch take effect now, on the writer's thread.
        foreach (ITableState table in _columns.Tables.Values)
        {
            table.Retention.ApplyRequest();
        }

        Retention retention = _root.Retention.Current;
        ReadOnlySpan<T> records = batch.Records;

        try
        {
            // A root window smaller than the batch keeps only its last records; the others would
            // be evicted at once, so they are skipped instead (ADR 0006, point 7).
            if (retention.Kind == RetentionKind.Latest && records.Length > retention.Items)
            {
                int skip = records.Length - (int)retention.Items;
                _root.SkipRange(records[..skip], batch.Payload);
                records = records[skip..];
            }

            _root.WriteRange(records, batch.Payload);
        }
        catch
        {
            _root.Rollback();
            throw;
        }

        // Evict before publishing, so readers never see skipped items as readable.
        _root.Retain(retention.Kind == RetentionKind.Latest ? _root.Written - retention.Items : 0);
        _root.Publish();
        Volatile.Write(ref _count, _root.Written);
    }

    /// <summary>
    /// Changes how much of one table stays in memory, while the run continues (ADR 0006, point 8).
    /// Safe from any thread; takes effect at the next batch.
    /// </summary>
    /// <remarks>
    /// Shrinking frees memory at once. Growing applies to new data: what was already evicted is
    /// gone, and the window fills up as items arrive. Growing beyond the window's prepared
    /// <see cref="Retention.Max"/> is allowed, and allocates. A nested table never keeps more than
    /// its parent, whatever is asked for.
    /// </remarks>
    /// <param name="table">The table's name.</param>
    /// <param name="retention">The new retention.</param>
    /// <exception cref="KeyNotFoundException">There is no such table.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="retention"/> is <see cref="Retention.SameAsParent"/> for the root table.
    /// </exception>
    public void SetRetention(string table, Retention retention)
    {
        TableSchema schema = FindTable(table);
        if (schema.Parent is null && retention.Kind == RetentionKind.SameAsParent)
        {
            throw new ArgumentException($"'{table}' is the root table, so it has no parent to follow.", nameof(retention));
        }

        _columns.Tables[table].Retention.Request(retention);
    }

    /// <summary>Returns a table's retention: the one being applied, or the one most recently asked for.</summary>
    /// <exception cref="KeyNotFoundException">There is no such table.</exception>
    public Retention GetRetention(string table)
    {
        FindTable(table);
        return _columns.Tables[table].Retention.Latest;
    }

    /// <summary>
    /// Returns the oldest item of a table still in memory. Items from here up to the table's count
    /// are readable in every one of its columns.
    /// </summary>
    /// <exception cref="KeyNotFoundException">There is no such table.</exception>
    public long GetFirstRetained(string table)
    {
        FindTable(table);
        return _columns.Tables[table].PublishedFirstRetained;
    }

    /// <summary>Returns what one table holds right now: its window, evictions and chunks.</summary>
    /// <exception cref="KeyNotFoundException">There is no such table.</exception>
    public TableMetrics GetTableMetrics(string table)
    {
        FindTable(table);
        ITableState state = _columns.Tables[table];

        long allocated = 0;
        long freed = 0;
        if (_columns.ColumnsOfTable.TryGetValue(table, out List<IColumn>? columns))
        {
            foreach (IColumn column in columns)
            {
                allocated += column.ChunksAllocated;
                freed += column.ChunksFreed;
            }
        }

        // First before count, so the window read is never wider than what was published together.
        long first = state.PublishedFirstRetained;
        long evictedOnArrival = state.PublishedEvictedOnArrival;
        long count = Math.Max(state.PublishedCount, first);

        return new TableMetrics(table, state.Retention.Latest, count, first, evictedOnArrival, allocated, freed);
    }

    /// <summary>Returns <see cref="GetTableMetrics(string)"/> for every table, root first.</summary>
    public IReadOnlyList<TableMetrics> GetTableMetrics()
    {
        var all = new TableMetrics[Schema.Tables.Count];
        for (int i = 0; i < all.Length; i++)
        {
            all[i] = GetTableMetrics(Schema.Tables[i].Name);
        }

        return all;
    }

    /// <summary>
    /// Returns one column of a <see cref="TableStorage.Columns"/> table.
    /// </summary>
    /// <typeparam name="TValue">The column's element type, as given to <c>Field</c>.</typeparam>
    /// <exception cref="KeyNotFoundException">There is no such table or field.</exception>
    /// <exception cref="ArgumentException">The column holds a different type than <typeparamref name="TValue"/>.</exception>
    public ChunkedColumn<TValue> GetColumn<TValue>(string table, string field)
        where TValue : unmanaged
    {
        if (!_columns.Fields.TryGetValue((table, field), out object? column))
        {
            throw new KeyNotFoundException($"There is no field '{field}' in table '{table}'.");
        }

        return column as ChunkedColumn<TValue>
            ?? throw new ArgumentException(
                $"Field '{table}.{field}' holds {ColumnType(column)}, not {typeof(TValue).Name}.", nameof(TValue));
    }

    /// <summary>
    /// Returns the rows of a <see cref="TableStorage.Rows"/> table.
    /// </summary>
    /// <typeparam name="TItem">The table's item type.</typeparam>
    /// <exception cref="KeyNotFoundException">There is no such Rows table.</exception>
    /// <exception cref="ArgumentException">The table holds a different type than <typeparamref name="TItem"/>.</exception>
    public ChunkedColumn<TItem> GetRows<TItem>(string table)
        where TItem : unmanaged
    {
        if (!_columns.Rows.TryGetValue(table, out object? rows))
        {
            throw new KeyNotFoundException($"There is no Rows table called '{table}'.");
        }

        return rows as ChunkedColumn<TItem>
            ?? throw new ArgumentException(
                $"Table '{table}' holds {ColumnType(rows)}, not {typeof(TItem).Name}.", nameof(TItem));
    }

    /// <summary>
    /// Returns the offsets of a nested table: parent item <c>i</c> owns the nested items from
    /// <c>offsets[i]</c> up to (not including) <c>offsets[i + 1]</c>. It holds one more value than
    /// there are parent items. Offsets are evicted along with their parent items.
    /// </summary>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    public ChunkedColumn<long> GetOffsets(string table)
    {
        return _columns.Offsets.TryGetValue(table, out ChunkedColumn<long>? offsets)
            ? offsets
            : throw new KeyNotFoundException($"There is no nested table called '{table}'.");
    }

    /// <summary>
    /// Finds which items of a nested table belong to one parent item, for example which samples
    /// are event 42's waveform, if they are still in memory.
    /// </summary>
    /// <param name="table">The nested table's name.</param>
    /// <param name="parentIndex">The parent item's index in its own table.</param>
    /// <param name="start">The first nested item's index.</param>
    /// <param name="count">How many nested items there are.</param>
    /// <returns>
    /// <see langword="true"/> if they are in memory; <see langword="false"/> if they, or the parent
    /// item, have been evicted.
    /// </returns>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="parentIndex"/> is negative or not appended yet.</exception>
    public bool TryGetChildRange(string table, long parentIndex, out long start, out long count)
    {
        ChunkedColumn<long> offsets = GetOffsets(table);
        long published = offsets.Count;
        ArgumentOutOfRangeException.ThrowIfNegative(parentIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(parentIndex, published - 1);

        start = 0;
        count = 0;
        if (parentIndex < offsets.FirstRetained)
        {
            return false;
        }

        try
        {
            start = offsets[parentIndex];
            count = offsets[parentIndex + 1] - start;
        }
        catch (ArgumentOutOfRangeException)
        {
            // Evicted between the check and the read.
            return false;
        }

        return start >= _columns.Tables[table].PublishedFirstRetained;
    }

    /// <summary>
    /// Returns which items of a nested table belong to one parent item, for example which samples
    /// are event 42's waveform.
    /// </summary>
    /// <param name="table">The nested table's name.</param>
    /// <param name="parentIndex">The parent item's index in its own table.</param>
    /// <returns>The first nested item's index, and how many there are.</returns>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="parentIndex"/> is out of range, or its items have been evicted.
    /// </exception>
    public (long Start, long Count) GetChildRange(string table, long parentIndex)
    {
        return TryGetChildRange(table, parentIndex, out long start, out long count)
            ? (start, count)
            : throw new ArgumentOutOfRangeException(
                nameof(parentIndex), parentIndex, $"The '{table}' items of item {parentIndex:N0} have been evicted.");
    }

    private TableSchema FindTable(string table) =>
        Schema.Find(table) ?? throw new KeyNotFoundException($"There is no table called '{table}'.");

    // The element type of a ChunkedColumn<X> held as object, for error messages.
    private static string ColumnType(object column) => column.GetType().GetGenericArguments()[0].Name;
}
