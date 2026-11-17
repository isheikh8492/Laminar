using Laminar.Core;

namespace Laminar.Ingestion.Layout;

/// <summary>
/// Starts a layout: a description of where each part of a record goes, and whether each part is
/// stored as rows or columns (ADR 0004).
/// </summary>
/// <example>
/// Scalars as columns, each event's waveform as rows:
/// <code>
/// var layout = RecordLayout.Columns&lt;EventPacket&gt;("events", t =&gt; t
///     .Field("time", p =&gt; p.Timestamp)
///     .Field("fsc", p =&gt; p.Fsc)
///     .Many("waveform", p =&gt; p.Waveform, TableStorage.Rows));
///
/// var store = new LayoutStore&lt;EventPacket&gt;(layout);
/// </code>
/// </example>
public static class RecordLayout
{
    /// <summary>
    /// Starts a layout whose root table stores chosen fields as columns.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <param name="name">The root table's name.</param>
    /// <param name="configure">Chooses the fields and nested tables.</param>
    /// <param name="retain">How many records stay in memory (ADR 0006). Everything, by default.</param>
    /// <exception cref="InvalidOperationException">The table ends up with no fields.</exception>
    /// <exception cref="ArgumentException"><paramref name="retain"/> is <see cref="Retention.SameAsParent"/>.</exception>
    public static RecordLayout<T> Columns<T>(string name, Action<TableBuilder<T>> configure, Retention retain = default)
        where T : unmanaged
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        return Build(name, TableStorage.Columns, retain, rowsPart: null, configure);
    }

    /// <summary>
    /// Starts a layout whose root table stores whole records as rows.
    /// </summary>
    /// <typeparam name="T">The record type; must be unmanaged to be stored whole.</typeparam>
    /// <param name="name">The root table's name.</param>
    /// <param name="configure">Adds nested tables, if any. Rows tables have no fields.</param>
    /// <param name="retain">How many records stay in memory (ADR 0006). Everything, by default.</param>
    /// <exception cref="ArgumentException"><paramref name="retain"/> is <see cref="Retention.SameAsParent"/>.</exception>
    public static RecordLayout<T> Rows<T>(string name, Action<TableBuilder<T>>? configure = null, Retention retain = default)
        where T : unmanaged
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return Build(name, TableStorage.Rows, retain, RowsPart<T>(name, TableStorage.Rows), configure);
    }

    // The part that stores whole items, for a Rows table; null for a Columns table. It lives here,
    // in a method with the unmanaged constraint, because TableBuilder<TOwner> can't require it.
    internal static Func<LayoutColumns, IPartWriter<TItem>>? RowsPart<TItem>(string table, TableStorage storage)
        where TItem : unmanaged
    {
        if (storage != TableStorage.Rows)
        {
            return null;
        }

        return columns =>
        {
            var rows = new ChunkedColumn<TItem>(columns.ChunkSize);
            columns.Rows[table] = rows;
            columns.Register(table, rows);
            return new RowWriter<TItem>(rows);
        };
    }

    private static RecordLayout<T> Build<T>(
        string name,
        TableStorage storage,
        Retention retain,
        Func<LayoutColumns, IPartWriter<T>>? rowsPart,
        Action<TableBuilder<T>>? configure)
        where T : unmanaged
    {
        var context = new LayoutContext();
        var root = new TableBuilder<T>(context, name, storage, parent: null, retain, rowsPart);
        configure?.Invoke(root);
        TableDefinition<T> definition = root.Complete();

        return new RecordLayout<T>(context.ToSchema(), definition);
    }
}

/// <summary>
/// A finished layout for records of type <typeparamref name="T"/>: the schema (plain data) plus
/// the accessors (code) that fill it (ADR 0004). Create one with <see cref="RecordLayout"/>, then
/// pass it to <see cref="LayoutStore{T}"/>. One layout can serve any number of stores.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class RecordLayout<T>
    where T : unmanaged
{
    internal RecordLayout(LayoutSchema schema, TableDefinition<T> root)
    {
        Schema = schema;
        Root = root;
    }

    /// <summary>Every table, its storage, fields and nesting, as plain data.</summary>
    public LayoutSchema Schema { get; }

    internal TableDefinition<T> Root { get; }
}
