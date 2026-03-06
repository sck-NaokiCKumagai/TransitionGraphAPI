using System.Reflection;
using System.Runtime.ExceptionServices;

namespace TransitionGraphDataEdit.Grid;

/// <summary>
/// Dummy (CSV-backed) implementation.
///
/// This class intentionally avoids compile-time dependency on the dummy DLL types because
/// the project can be built in a production configuration where only the production DLL is referenced.
/// </summary>
public sealed class DummyGridClient : IGridClient
{
    private readonly object _impl;
    private readonly MethodInfo _miGetColumn;
    private readonly MethodInfo _miGetGridData;
    private readonly MethodInfo _miGetFilter;

    public DummyGridClient(string csvDirectory)
    {
        // Dummy DLL provides DXC.EngPF.Core.Grid.GridClient (not present in production DLL).
        var t = Type.GetType("DXC.EngPF.Core.Grid.GridClient, DXC.EngPF.Core.Grid", throwOnError: false);
        if (t == null)
            throw new InvalidOperationException("Dummy GridClient type was not found. Ensure DummyDLL is referenced when UseDummy=true.");

        _impl = Activator.CreateInstance(t, csvDirectory)
            ?? throw new InvalidOperationException("Failed to create dummy GridClient instance.");

        _miGetColumn = t.GetMethod("GetColumnDataAndRowCountAsync", new[] { typeof(string), typeof(CancellationToken) })
            ?? throw new MissingMethodException(t.FullName, "GetColumnDataAndRowCountAsync");
        _miGetGridData = t.GetMethod(
                "GetGridDataAsync",
                new[] { typeof(string[]), typeof(string), typeof(bool), typeof(int), typeof(int), typeof(int), typeof(int), typeof(CancellationToken) })
            ?? throw new MissingMethodException(t.FullName, "GetGridDataAsync");
        _miGetFilter = t.GetMethod("GetGridFilterInfoAsync", new[] { typeof(string), typeof(CancellationToken) })
            ?? throw new MissingMethodException(t.FullName, "GetGridFilterInfoAsync");
    }

    public async Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken)
    {
        var taskObj = InvokeUnwrap(_miGetColumn, _impl, new object?[] { secondCacheId, cancellationToken })
            ?? throw new InvalidOperationException("Dummy GetColumnDataAndRowCountAsync returned null.");
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapColumnDataAndRowCount(raw);
    }

    public async Task<GridDataResult> GetGridDataAsync(string[] columnIds, string gridId, string sessionId, bool isGetVirtualColumnValue, int height, int width, int x, int y, CancellationToken cancellationToken)
    {
        var taskObj = InvokeUnwrap(_miGetGridData, _impl, new object?[] { columnIds, gridId, isGetVirtualColumnValue, height, width, x, y, cancellationToken })
            ?? throw new InvalidOperationException("Dummy GetGridDataAsync returned null.");
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapGridDataResult(raw);
    }

    public async Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken)
    {
        var taskObj = InvokeUnwrap(_miGetFilter, _impl, new object?[] { secondCacheId, cancellationToken })
            ?? throw new InvalidOperationException("Dummy GetGridFilterInfoAsync returned null.");
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapGridFilterInfoResult(raw);
    }

    public Task<string> SetFilterAndSortAsync(FilterAndSortParam param, string sessionId, CancellationToken cancellationToken)
        => Task.FromResult("Accept Request");

    // -----------------
    // Reflection helpers
    // -----------------

    private static async Task<object> CastTaskToObject(object taskObj)
    {
        if (taskObj is not Task task)
            throw new InvalidOperationException("Expected a Task return type.");

        await task.ConfigureAwait(false);

        var t = taskObj.GetType();
        if (!t.IsGenericType) return new object();

        var resultProp = t.GetProperty("Result");
        return resultProp?.GetValue(taskObj) ?? new object();
    }

    private static ColumnDataAndRowCountResult MapColumnDataAndRowCount(object raw)
    {
        var outObj = new ColumnDataAndRowCountResult();
        var t = raw.GetType();

        outObj.RowCount = (int)(t.GetProperty("RowCount")?.GetValue(raw) ?? 0);

        var colArrayObj = t.GetProperty("ColumnDataArray")?.GetValue(raw)
                          ?? t.GetProperty("ColumnDataList")?.GetValue(raw);

        if (colArrayObj is System.Collections.IEnumerable en)
        {
            foreach (var item in en)
            {
                if (item == null) continue;
                outObj.ColumnDataArray.Add(MapDataItemColumnData(item));
            }
        }

        return outObj;
    }

    private static DataItemColumnData MapDataItemColumnData(object raw)
    {
        var t = raw.GetType();
        return new DataItemColumnData
        {
            ColumnId = (string?)t.GetProperty("ColumnId")?.GetValue(raw) ?? string.Empty,
            DataItemKind = (string?)t.GetProperty("DataItemKind")?.GetValue(raw),
            DisplayString = (string?)t.GetProperty("DisplayString")?.GetValue(raw),
            IsHeader = (bool?)(t.GetProperty("IsHeader")?.GetValue(raw)) ?? false,
            IsNnumeric = (bool?)(t.GetProperty("IsNnumeric")?.GetValue(raw)) ?? false,
        };
    }

    private static GridDataResult MapGridDataResult(object raw)
    {
        var t = raw.GetType();
        var outObj = new GridDataResult();

        var gridDataObj = t.GetProperty("GridData")?.GetValue(raw);
        if (gridDataObj is System.Collections.IEnumerable cols)
        {
            foreach (var col in cols)
            {
                if (col is System.Collections.IEnumerable rows)
                {
                    var list = new List<object?>();
                    foreach (var cell in rows) list.Add(cell);
                    outObj.GridData.Add(list);
                }
            }
        }

        var idxObj = t.GetProperty("ColumnIndexArray")?.GetValue(raw);
        if (idxObj is System.Collections.IEnumerable idxEn)
        {
            foreach (var v in idxEn)
            {
                if (v == null) continue;
                if (v is int i) outObj.ColumnIndexArray.Add(i);
                else if (int.TryParse(v.ToString(), out var ii)) outObj.ColumnIndexArray.Add(ii);
            }
        }

        return outObj;
    }

    private static GridFilterInfoResult MapGridFilterInfoResult(object raw)
    {
        var t = raw.GetType();
        var outObj = new GridFilterInfoResult();

        var listObj = t.GetProperty("FilterInfoList")?.GetValue(raw);
        if (listObj is System.Collections.IEnumerable en)
        {
            foreach (var item in en)
            {
                if (item == null) continue;
                var it = item.GetType();
                outObj.FilterInfoList.Add(new FilterInfoString
                {
                    ColumnName = (string?)it.GetProperty("ColumnName")?.GetValue(item),
                    FilterString = (string?)it.GetProperty("FilterString")?.GetValue(item),
                    IsCurrentFilter = (bool?)(it.GetProperty("IsCurrentFilter")?.GetValue(item)) ?? false,
                });
            }
        }

        return outObj;
    }


    private static object InvokeUnwrap(MethodInfo mi, object instance, object?[] args)
    {
        try
        {
            return mi.Invoke(instance, args) ?? throw new InvalidOperationException($"{mi.Name} returned null.");
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
            throw; // unreachable
        }
    }
}
