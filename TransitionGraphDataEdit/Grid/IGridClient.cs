namespace TransitionGraphDataEdit.Grid;

/// <summary>
/// Abstraction over the Grid DLL.
///
/// IMPORTANT:
/// - The production DLL and the dummy DLL have incompatible public type names.
/// - This interface uses internal models (GridModels.cs) to keep the rest of the app stable.
/// </summary>
public interface IGridClient
{
    Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken);

    Task<GridDataResult> GetGridDataAsync(
        string[] columnIds,
        string gridId,
        string sessionId,
        bool isGetVirtualColumnValue,
        int height,
        int width,
        int x,
        int y,
        CancellationToken cancellationToken);

    Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken);

    /// <summary>
    /// RequestId=10: Set filter and sort state for a 2nd cache.
    /// Returns the fixed string "Accept Request" per spec.
    /// </summary>
    Task<string> SetFilterAndSortAsync(FilterAndSortParam param, string sessionId, CancellationToken cancellationToken);
}
