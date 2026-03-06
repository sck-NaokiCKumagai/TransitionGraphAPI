namespace TransitionGraphDataEdit.Grid;

/// <summary>
/// Internal models to decouple TransitionGraphDataEdit from the production/dummy Grid DLL type differences.
///
/// NOTE:
/// - Both the production DLL and the dummy DLL expose similar concepts but with different type names.
/// - These models represent the subset actually used by this service.
/// </summary>
public sealed class ColumnDataAndRowCountResult
{
    public List<DataItemColumnData> ColumnDataArray { get; set; } = new();
    public int RowCount { get; set; }
}

public sealed class DataItemColumnData
{
    public string ColumnId { get; set; } = string.Empty;
    public string? ColumnName { get; set; }
    public string? DataItemKind { get; set; }
    public string? DisplayString { get; set; }
    public bool IsHeader { get; set; }
    public bool IsNnumeric { get; set; }

    // Keep the rest as optional to preserve compatibility with CSV dummy parsing.
    public string? LayerName { get; set; }
    public string? DataItemId { get; set; }
    public string? DataItemName { get; set; }
    public string? Unit { get; set; }
    public bool Visible { get; set; } = true;
    public bool IsFixed { get; set; }
    public ICollection<string> DispNameList { get; set; } = new List<string>();
    public ICollection<string> KindList { get; set; } = new List<string>();
    public string? Format { get; set; }
    //public string? ColumnName { get; set; }
    public string? DisplayNameFormat { get; set; }
    public string? LineDispNameFormat { get; set; }
    public GridTableColumnType ColunType { get; set; }
    public bool IsDateTime { get; set; }
    public string? SecondColumnId { get; set; }
    public string? DispName { get; set; }
    public string? LineDispName { get; set; }
    public GridTableColumnType GridTableColumnType { get; set; }
    public bool IsDoubleValue { get; set; }
    public bool IsFloatValue { get; set; }
    public bool IsIntValue { get; set; }
    public int BinarySerializeClassVersion { get; set; }
    public string? StringFormat { get; set; }
}

public enum GridTableColumnType
{
    _0 = 0,
    _1 = 1,
    _2 = 2,
    _3 = 3,
    _4 = 4,
    _5 = 5,
    _6 = 6,
    _7 = 7,
    _12 = 12,
    _13 = 13,
    _14 = 14,
    _15 = 15,
    _16 = 16,
    _23 = 23,
    _30 = 30,
    _40 = 40,
    _41 = 41,
    _42 = 42,
    _43 = 43,
    _44 = 44,
}

public sealed class GridDataResult
{
    public ICollection<ICollection<object?>> GridData { get; set; } = new List<ICollection<object?>>();
    public ICollection<int> ColumnIndexArray { get; set; } = new List<int>();
}

public sealed class GridFilterInfoResult
{
    public ICollection<FilterInfoString> FilterInfoList { get; set; } = new List<FilterInfoString>();
}

public sealed class FilterInfoString
{
    public string? ColumnName { get; set; }
    public string? FilterString { get; set; }
    public bool IsCurrentFilter { get; set; }
}

public sealed class FilterAndSortParam
{
    public string SecondCacheId { get; set; } = string.Empty;
    public string SortConditionParameter { get; set; } = string.Empty;
    public bool IsManualSort { get; set; }
    public int ManualSortMoveIndex { get; set; }
    public ICollection<int> ManualSortTargetRowIndex { get; set; } = new List<int>();
}
