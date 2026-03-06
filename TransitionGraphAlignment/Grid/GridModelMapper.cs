using System.Collections;
using System.Reflection;

namespace TransitionGraphAlignment.Grid;

/// <summary>
/// Maps production (DXC.EngPF.Core.Grid.*) DTO objects (obtained via reflection) into Alignment's internal models.
/// This class must NOT reference any DXC.* assemblies directly.
/// </summary>
internal static class GridModelMapper
{
    public static ColumnDataAndRowCountResult FromRawColumnDataAndRowCount(object? raw)
    {
        var result = new ColumnDataAndRowCountResult();
        if (raw is null) return result;

        result.RowCount = ReadInt(raw, "RowCount")
                          ?? ReadInt(raw, "rowCount")
                          ?? 0;

        var colsObj =
            ReadProp(raw, "ColumnDataArray")
            ?? ReadProp(raw, "ColumnData")
            ?? ReadProp(raw, "ColumnDataList");

        foreach (var item in Enumerate(colsObj))
        {
            if (item is null) continue;
            result.ColumnDataArray.Add(MapColumn(item));
        }

        return result;
    }

    public static GridDataResult FromRawGridData(object? raw)
    {
        var result = new GridDataResult();
        if (raw is null) return result;

        var gridDataObj =
            ReadProp(raw, "GridData")
            ?? ReadProp(raw, "gridData");

        var outer = new List<ICollection<object?>>();
        foreach (var col in Enumerate(gridDataObj))
        {
            var inner = new List<object?>();
            foreach (var cell in Enumerate(col))
            {
                inner.Add(cell);
            }
            outer.Add(inner);
        }
        result.GridData = outer;

        var colIdxObj =
            ReadProp(raw, "ColumnIndexArray")
            ?? ReadProp(raw, "ColumnIndex")
            ?? ReadProp(raw, "ColumnIndexList");

        var idxList = new List<int>();
        foreach (var v in Enumerate(colIdxObj))
        {
            if (v is null) continue;
            if (v is int i) idxList.Add(i);
            else if (int.TryParse(v.ToString(), out var parsed)) idxList.Add(parsed);
        }

        result.ColumnIndexArray = idxList.Count > 0 ? idxList.ToArray() : null;
        return result;
    }

    public static GridFilterInfoResult FromRawGridFilterInfo(object? raw)
    {
        var result = new GridFilterInfoResult();
        if (raw is null) return result;

        var s = ReadString(raw, "FilterInfo") ?? ReadString(raw, "filterInfo");
        if (!string.IsNullOrEmpty(s)) result.FilterInfo = s;

        var listObj =
            ReadProp(raw, "FilterInfoList")
            ?? ReadProp(raw, "FilterInfoArray")
            ?? ReadProp(raw, "FilterInfo")
            ?? ReadProp(raw, "filterInfoList");

        foreach (var item in Enumerate(listObj))
        {
            if (item is null) continue;
            var fi = new FilterInfoString
            {
                ColumnName = ReadString(item, "ColumnName") ?? ReadString(item, "columnName"),
                FilterString = ReadString(item, "FilterString") ?? ReadString(item, "filterString"),
                IsCurrentFilter = ReadBool(item, "IsCurrentFilter") ?? ReadBool(item, "isCurrentFilter") ?? false
            };

            if (fi.ColumnName is null && fi.FilterString is null && fi.IsCurrentFilter == false)
                continue;

            result.FilterInfoList.Add(fi);
        }

        return result;
    }

    private static DataItemColumnData MapColumn(object rawCol)
    {
        var col = new DataItemColumnData
        {
            ColumnId = ReadString(rawCol, "ColumnId") ?? ReadString(rawCol, "columnId") ?? string.Empty,
            ColumnName = ReadString(rawCol, "ColumnName") ?? ReadString(rawCol, "columnName"),
            DataItemKind = ReadString(rawCol, "DataItemKind") ?? ReadString(rawCol, "dataItemKind"),
            DisplayString = ReadString(rawCol, "DisplayString") ?? ReadString(rawCol, "displayString"),
            IsHeader = ReadBool(rawCol, "IsHeader") ?? ReadBool(rawCol, "isHeader") ?? false,
            IsNnumeric = ReadBool(rawCol, "IsNnumeric") ?? ReadBool(rawCol, "isNnumeric") ?? false,
            LayerName = ReadString(rawCol, "LayerName") ?? ReadString(rawCol, "layerName"),
            DataItemId = ReadString(rawCol, "DataItemId") ?? ReadString(rawCol, "dataItemId"),
            DataItemName = ReadString(rawCol, "DataItemName") ?? ReadString(rawCol, "dataItemName"),
            Unit = ReadString(rawCol, "Unit") ?? ReadString(rawCol, "unit"),
            Visible = ReadBool(rawCol, "Visible") ?? ReadBool(rawCol, "visible") ?? true
        };

        var displayName = ReadString(rawCol, "DisplayName") ?? ReadString(rawCol, "displayName");
        if (!string.IsNullOrEmpty(displayName))
        {
            col.DisplayString = displayName;
        }

        if (string.IsNullOrWhiteSpace(col.ColumnName))
        {
            col.ColumnName = col.DisplayString ?? col.DataItemName;
        }

        return col;
    }

    private static object? ReadProp(object obj, string name)
        => obj.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)?.GetValue(obj);

    private static string? ReadString(object obj, string name)
        => ReadProp(obj, name) as string;

    private static int? ReadInt(object obj, string name)
    {
        var v = ReadProp(obj, name);
        if (v is null) return null;
        if (v is int i) return i;
        if (int.TryParse(v.ToString(), out var parsed)) return parsed;
        return null;
    }

    private static bool? ReadBool(object obj, string name)
    {
        var v = ReadProp(obj, name);
        if (v is null) return null;
        if (v is bool b) return b;
        if (bool.TryParse(v.ToString(), out var parsed)) return parsed;
        return null;
    }

    private static IEnumerable<object?> Enumerate(object? maybeEnumerable)
    {
        if (maybeEnumerable is null) yield break;
        if (maybeEnumerable is string) yield break; // treat string as scalar

        if (maybeEnumerable is IEnumerable e)
        {
            foreach (var item in e) yield return item;
        }
    }
}
