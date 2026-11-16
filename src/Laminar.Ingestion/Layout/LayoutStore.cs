using Laminar.Core;

namespace Laminar.Ingestion.Layout;

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
/// <b>Failure:</b> if an accessor throws partway through a batch, everything written for that
/// batch is rolled back before the exception continues, so the tables stay in step.
/// </para>
/// <para>
/// <b>Threading:</b> one writer (the ingestion worker) calls <see cref="Append"/>. Any number of
/// readers may use <see cref="Count"/> and the columns at the same time, without locks.
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

    /// <summary>Every table, its storage, fields and nesting.</summary>
    public LayoutSchema Schema { get; }

    /// <inheritdoc/>
    /// <remarks>The number of records, which is the number of items in the root table.</remarks>
    public long Count => Volatile.Read(ref _count);

    /// <inheritdoc/>
    /// <remarks>
    /// Reserves every column of the root table, and the offsets of each nested table. Nested items
    /// aren't reserved here, because how many each record has isn't known; reserve those with
    /// <see cref="ReserveNested"/>. Must be called from the writer's thread, or before appending starts.
    /// </remarks>
    public void Reserve(long records)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(records);
        _root.Reserve(records);
    }

    /// <summary>
    /// Reserves room for <paramref name="items"/> items in one nested table, for example the total
    /// number of waveform samples expected: records × average samples per record.
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
        TableSchema schema = Schema.Find(table) ?? throw new KeyNotFoundException($"There is no table called '{table}'.");
        if (schema.Parent is null)
        {
            throw new KeyNotFoundException($"'{table}' is the root table; reserve it with Reserve.");
        }

        if (_columns.Rows.TryGetValue(table, out object? rows))
        {
            ((IReservable)rows).Reserve(items);
        }

        foreach (FieldSchema field in schema.Fields)
        {
            ((IReservable)_columns.Fields[(table, field.Name)]).Reserve(items);
        }
    }

    /// <inheritdoc/>
    /// <remarks>Must only be called from one thread at a time.</remarks>
    public void Append(Batch<T> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        try
        {
            _root.WriteRange(batch.Records, batch.Payload);
        }
        catch
        {
            _root.Rollback();
            throw;
        }

        _root.Publish();
        Volatile.Write(ref _count, _root.Written);
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
    /// there are parent items.
    /// </summary>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    public ChunkedColumn<long> GetOffsets(string table)
    {
        return _columns.Offsets.TryGetValue(table, out ChunkedColumn<long>? offsets)
            ? offsets
            : throw new KeyNotFoundException($"There is no nested table called '{table}'.");
    }

    /// <summary>
    /// Returns which items of a nested table belong to one parent item, for example which samples
    /// are event 42's waveform.
    /// </summary>
    /// <param name="table">The nested table's name.</param>
    /// <param name="parentIndex">The parent item's index in its own table.</param>
    /// <returns>The first nested item's index, and how many there are.</returns>
    /// <exception cref="KeyNotFoundException">There is no such nested table.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="parentIndex"/> is out of range.</exception>
    public (long Start, long Count) GetChildRange(string table, long parentIndex)
    {
        ChunkedColumn<long> offsets = GetOffsets(table);
        ArgumentOutOfRangeException.ThrowIfNegative(parentIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(parentIndex, offsets.Count - 1);

        long start = offsets[parentIndex];
        return (start, offsets[parentIndex + 1] - start);
    }

    // The element type of a ChunkedColumn<X> held as object, for error messages.
    private static string ColumnType(object column) => column.GetType().GetGenericArguments()[0].Name;
}
