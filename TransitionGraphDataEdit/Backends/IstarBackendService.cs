using System.Text.Json;
using TransitionGraph.Dtos;
using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphDataEdit.Grid;
using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// RequestId=7 builder.
///
/// - First triggers Alignment (sync with external system).
/// - Then reads grid data via DXC.EngPF.Core.Grid.dll (dummy CSV implementation available) and builds ISTARInfoDto.
/// - Persists result as single JSON row in ResultStore.
/// </summary>
public sealed class IstarBackendService : IBackendService
{
    private readonly DataDir _dir;
    private readonly AlignmentBackendService _alignment;
    private readonly IGridClient _grid;

    public IstarBackendService(DataDir dir, AlignmentBackendService alignment, IGridClient grid)
    {
        _dir = dir;
        _alignment = alignment;
        _grid = grid;
    }

    public async Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken)
    {
        // 0) Sync with external system (best-effort)
        try
        {
            await _alignment.BuildAsync(request, resultId, cancellationToken);
        }
        catch
        {
            // Alignment is for synchronization; even if it fails, still try to build the dataset locally.
        }

        // parameter mapping (spec): Parm1=GUID, Parm2=SecondCacheId, Parm3=granularity, Parm4=sessionId
        request.Parameters.TryGetValue("Parm2", out var secondCacheId);
        if (string.IsNullOrWhiteSpace(secondCacheId))
            secondCacheId = request.Parameters.TryGetValue("gridId", out var g) ? g : string.Empty;

        request.Parameters.TryGetValue("Parm4", out var sessionId);
        sessionId ??= string.Empty;

        // (1) GetColumnDataAndRowCountAsync
        var colAndCount = await _grid.GetColumnDataAndRowCountAsync(secondCacheId ?? string.Empty, sessionId, cancellationToken);
        var rowCount = colAndCount.RowCount;
        var colCount = colAndCount.ColumnDataArray.Count;
        var columnIds = colAndCount.ColumnDataArray.Select(x => x.ColumnId).ToArray();

        // (2) GetGridDataAsync
        var gridData = await _grid.GetGridDataAsync(
            columnIds,
            secondCacheId ?? string.Empty,
            sessionId,
            isGetVirtualColumnValue: true,
            height: rowCount,
            width: colCount,
            x: 0,
            y: 0,
            cancellationToken);

        // (3) LotNameList
        int lotNameColumnIndex = colAndCount.ColumnDataArray
            .Select((item, i) => new { item, i })
            .Where(x => string.Equals(x.item.ColumnId, "LOT_NAME", StringComparison.OrdinalIgnoreCase))
            .Select(x => x.i)
            .DefaultIfEmpty(-1)
            .First();

        var gridColumns = gridData.GridData as IList<ICollection<object?>> ?? gridData.GridData.ToList();
        var lotNameList = new List<string>();
        if (lotNameColumnIndex >= 0 && lotNameColumnIndex < gridColumns.Count)
        {
            var columnObjects = gridColumns[lotNameColumnIndex];
            lotNameList = columnObjects.Select(v => v?.ToString() ?? string.Empty).ToList();
        }

        // (4) ItemNameList
        var itemNameList = new List<string>();
        foreach (var dataItemColumnData in colAndCount.ColumnDataArray)
        {
            if (!dataItemColumnData.IsHeader && dataItemColumnData.IsNnumeric)
            {
                itemNameList.Add(dataItemColumnData.ColumnId);
            }
        }

        // (5) CateNameList
        var targetKinds = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "CATEGORY",
            "DATA_ITEM_KIND_WAFERMAP_INFO",
            "DATA_ITEM_KIND_SLOT_INFO",
            "DATA_ITEM_KIND_SITE_INFO"
        };
        var cateNameList = new List<string>();
        foreach (var dataItemColumnData in colAndCount.ColumnDataArray)
        {
            if (targetKinds.Contains(dataItemColumnData.DataItemKind ?? string.Empty))
            {
                cateNameList.Add(dataItemColumnData.ColumnId);
            }
        }

        // (6) FilterInfo
        var filterInfoResult = await _grid.GetGridFilterInfoAsync(secondCacheId ?? string.Empty, sessionId, cancellationToken);
        var filterInfoString = filterInfoResult.FilterInfoList.FirstOrDefault();
        var filterInfo = (filterInfoString != null &&
                          !string.IsNullOrWhiteSpace(filterInfoString.ColumnName) &&
                          !string.IsNullOrWhiteSpace(filterInfoString.FilterString))
            ? $"{filterInfoString.ColumnName}: {filterInfoString.FilterString}"
            : string.Empty;

        // LotInfoList (best-effort from columns)
        var lotInfoList = BuildLotInfoList(colAndCount, gridColumns, rowCount);

        var dto = new ISTARInfoDto
        {
            LotNameList = lotNameList,
            ItemNameList = itemNameList,
            CateNameList = cateNameList,
            FilterInfo = filterInfo,
            LotInfoList = lotInfoList
        };

        var json = JsonSerializer.Serialize(dto);
        ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, json) });

        return new BuildReply
        {
            Header = request.Header,
            ResultId = resultId,
            TotalCount = 1,
            Message = "Ready"
        };
    }

    public Task CancelAsync(RequestState state, CancellationToken cancellationToken)
        => _alignment.CancelAsync(state, cancellationToken);

    private static List<LotInfo> BuildLotInfoList(ColumnDataAndRowCountResult colAndCount, IList<ICollection<object?>> gridColumns, int rowCount)
    {
        int idxLot = FindColumnIndex(colAndCount, "LOT_NAME");
        int idxProd = FindColumnIndex(colAndCount, "PRODUCT_INFO");
        int idxWaferNo = FindColumnIndex(colAndCount, "WAFER_NO");
        int idxWaferId = FindColumnIndex(colAndCount, "WAFER_ID");
        int idxStd = FindColumnIndex(colAndCount, "STANDARD_PROC");
        if (idxStd < 0) idxStd = FindColumnIndex(colAndCount, "STANDERD_PROC");
        int idxProc = FindColumnIndex(colAndCount, "PROCESS");

        // If the minimum required column isn't present, return empty.
        if (idxLot < 0) return new List<LotInfo>();

        object? GetCell(int col, int row)
        {
            if (col < 0 || col >= gridColumns.Count) return null;
            var c = gridColumns[col];
            if (c is IList<object?> list)
                return (row >= 0 && row < list.Count) ? list[row] : null;
            // fallback
            return c.Skip(row).FirstOrDefault();
        }

        var listOut = new List<LotInfo>();
        for (int r = 0; r < rowCount; r++)
        {
            var lotName = GetCell(idxLot, r)?.ToString() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(lotName)) continue;

            int waferNo = 0;
            var waferNoStr = GetCell(idxWaferNo, r)?.ToString();
            if (!string.IsNullOrWhiteSpace(waferNoStr)) int.TryParse(waferNoStr, out waferNo);

            listOut.Add(new LotInfo
            {
                LotName = lotName,
                ProductInfo = GetCell(idxProd, r)?.ToString() ?? string.Empty,
                WaferNo = waferNo,
                WaferId = GetCell(idxWaferId, r)?.ToString() ?? string.Empty,
                StanderdProc = GetCell(idxStd, r)?.ToString() ?? string.Empty,
                Process = GetCell(idxProc, r)?.ToString() ?? string.Empty,
            });
        }
        return listOut;
    }

    private static int FindColumnIndex(ColumnDataAndRowCountResult colAndCount, string columnId)
    {
        return colAndCount.ColumnDataArray
            .Select((item, i) => new { item, i })
            .Where(x => string.Equals(x.item.ColumnId, columnId, StringComparison.OrdinalIgnoreCase))
            .Select(x => x.i)
            .DefaultIfEmpty(-1)
            .First();
    }
}
