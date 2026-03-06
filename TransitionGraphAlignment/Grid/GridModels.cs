namespace TransitionGraphAlignment.Grid;

/// <summary>
/// Internal models to decouple TransitionGraphAlignment from the production/dummy Grid DLL type differences.
///
/// NOTE:
/// - Production DLL (DXC.EngPF.Core.Grid.dll) and dummy DLL expose similar concepts but different public types.
/// - These models represent only the subset actually used by Alignment (RequestId=7/8/9/10).
/// </summary>
public sealed class ColumnDataAndRowCountResult
{
    // NOTE: Request8/9 helper methods expect IReadOnlyList<DataItemColumnData>.
    // Use List<> here so callers can pass ColumnDataArray directly.
    public List<DataItemColumnData> ColumnDataArray { get; set; } = new();
    public int RowCount { get; set; }
}

public sealed class DataItemColumnData
{
    public string ColumnId { get; set; } = string.Empty;

    // Some code paths refer to ColumnName (matching production DTO naming).
    // Keep it as an alias-friendly field; implementations can populate it from DisplayString/DataItemName.
    public string? ColumnName { get; set; }
    public string? DataItemKind { get; set; }
    public string? DisplayString { get; set; }
    public bool IsHeader { get; set; }
    public bool IsNnumeric { get; set; }

    // Optional fields for dummy CSV compatibility.
    public string? LayerName { get; set; }
    public string? DataItemId { get; set; }
    public string? DataItemName { get; set; }
    public string? Unit { get; set; }
    public bool Visible { get; set; } = true;

    public string? DisplayName
    {
        get => DisplayString;
        set => DisplayString = value;
    }
}

public sealed class GridDataResult
{
    /// <summary>
    /// Column-major 2D data: GridData[colIndex][rowIndex]
    /// </summary>
    public ICollection<ICollection<object?>> GridData { get; set; } = new List<ICollection<object?>>();

    /// <summary>
    /// Optional: returned by some implementations.
    /// </summary>
    public int[]? ColumnIndexArray { get; set; }
}

public sealed class GridFilterInfoResult
{
    public string? FilterInfo { get; set; }
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
