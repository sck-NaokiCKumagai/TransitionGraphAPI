namespace TransitionGraphAlignment.Grid;

/// <summary>
/// Grid access abstraction for Alignment.
///
/// IMPORTANT:
/// Alignment must support switching between a CSV-backed dummy client and the production
/// DXC grid client. To avoid type identity conflicts (dummy vs production assemblies),
/// this interface uses ONLY Alignment's internal models (see GridModels.cs).
/// </summary>
public interface IGridClient
{
    Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, CancellationToken cancellationToken);

    Task<GridDataResult> GetGridDataAsync(
        string[] columnIds,
        string gridId,
        bool isGetVirtualColumnValue,
        int height,
        int width,
        int x,
        int y,
        CancellationToken cancellationToken);

    Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, CancellationToken cancellationToken);

    /// <summary>
    /// RequestId=10: Set filter and sort state for a 2nd cache.
    ///
    /// Note:
    /// - Production environment uses DXC.EngPF.Core.Grid.dll via reflection.
    /// - Dev/test environment uses the dummy client (CSV).
    /// - Both implementations return the fixed string "Accept Request" per spec.
    /// </summary>
    Task<string> SetFilterAndSortAsync(FilterAndSortParam param, CancellationToken cancellationToken);
}
