namespace Laminar.Core;

/// <summary>
/// How a table in a layout stores its items (ADR 0004).
/// </summary>
public enum TableStorage
{
    /// <summary>
    /// Whole items side by side. Best for reading one item completely, such as one waveform or
    /// one header. The table has no fields; its <see cref="TableSchema.ItemType"/> is the row.
    /// </summary>
    Rows,

    /// <summary>
    /// One array per chosen field. Best for analysis that reads a few fields across many items,
    /// such as a histogram of one parameter.
    /// </summary>
    Columns,
}

/// <summary>
/// One column of a <see cref="TableStorage.Columns"/> table: its name and element type.
/// </summary>
/// <param name="Name">The field's name, unique within its table.</param>
/// <param name="Type">The element type, such as <see cref="float"/>.</param>
public sealed record FieldSchema(string Name, Type Type);

/// <summary>
/// One table of a layout: plain data describing it, with no code (ADR 0004, point 7).
/// </summary>
/// <param name="Name">The table's name, unique within the layout.</param>
/// <param name="Storage">Whether the table stores rows or columns.</param>
/// <param name="ItemType">
/// The type of one item in the table: the record type for the root table, the nested item type for
/// a table created with <c>Many</c>. For a <see cref="TableStorage.Rows"/> table, this is what each
/// row holds.
/// </param>
/// <param name="Parent">
/// The table this one is nested in, or <see langword="null"/> for the root table. A nested table
/// also has an offsets array: parent item <c>i</c> owns this table's items <c>offsets[i]</c> to
/// <c>offsets[i + 1]</c>.
/// </param>
/// <param name="Fields">The columns, in order. Empty for a <see cref="TableStorage.Rows"/> table.</param>
/// <param name="Retention">
/// How much of the table stays in memory, as declared in the layout (ADR 0006). It can change
/// during a run; the store reports the current one.
/// </param>
public sealed record TableSchema(
    string Name,
    TableStorage Storage,
    Type ItemType,
    string? Parent,
    IReadOnlyList<FieldSchema> Fields,
    Retention Retention = default);

/// <summary>
/// The plain-data half of a layout: every table, its storage, fields and nesting (ADR 0004). It
/// holds no code, so it can later be written into file headers or read from other languages.
/// </summary>
/// <param name="Tables">Every table; the root table comes first, then nested tables.</param>
public sealed record LayoutSchema(IReadOnlyList<TableSchema> Tables)
{
    /// <summary>The root table: one item per record.</summary>
    public TableSchema Root => Tables[0];

    /// <summary>Returns the table called <paramref name="name"/>, or <see langword="null"/> if there is none.</summary>
    public TableSchema? Find(string name)
    {
        foreach (TableSchema table in Tables)
        {
            if (table.Name == name)
            {
                return table;
            }
        }

        return null;
    }
}
