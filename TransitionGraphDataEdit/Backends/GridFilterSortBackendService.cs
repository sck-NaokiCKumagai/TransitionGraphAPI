using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphDataEdit.Grid;
using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// RequestId=10 backend.
///
/// Spec:
/// - parameter1: GUID (Parm1)
/// - parameter2: 2nd cache id (Parm2)
/// - parameter3: sort string (Parm3)
/// - parameter4: sessionId (Parm4)
///
/// DataEdit builds FilterAndSortParam and calls SetFilterAndSortAsync.
/// The returned request answer is the fixed string "Accept Request".
/// </summary>
public sealed class GridFilterSortBackendService : IBackendService
{
    private readonly DataDir _dir;
    private readonly IGridClient _grid;

    public GridFilterSortBackendService(DataDir dir, IGridClient grid)
    {
        _dir = dir;
        _grid = grid;
    }

    public async Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken)
    {
        var header = request.Header ?? new RequestHeader();

        // RequestId=10 mapping (parameter indexes are 1-based in the spec)
        // parameter[0] (Parm1) is GUID (already validated by caller)
        request.Parameters.TryGetValue("Parm2", out var secondCacheId);
        request.Parameters.TryGetValue("Parm3", out var sortStr);
        request.Parameters.TryGetValue("Parm4", out var sessionId);
        sessionId ??= string.Empty;

        var dto = new FilterAndSortParam
        {
            SecondCacheId = secondCacheId ?? string.Empty,
            SortConditionParameter = sortStr ?? string.Empty,
            IsManualSort = false,
            ManualSortMoveIndex = 0,
            ManualSortTargetRowIndex = new List<int>()
        };

        // (1)-(3) Execute DLL method (real/dummy switching handled by DI)
        _ = await _grid.SetFilterAndSortAsync(dto, sessionId, cancellationToken);

        // (4) Fixed request answer
        ResultStore.WriteAll(_dir.Path, resultId, new List<(int RowNo, string Value)> { (1, "Accept Request") });

        return new BuildReply
        {
            Header = header,
            ResultId = resultId,
            TotalCount = 1,
            Message = "Ready (Accept Request)"
        };
    }

    public Task CancelAsync(RequestState state, CancellationToken cancellationToken)
    {
        // No downstream cancellation for this request.
        return Task.CompletedTask;
    }
}
