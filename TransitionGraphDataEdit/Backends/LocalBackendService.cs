using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// Local (in-process) backend used for fixed responses and small helper requests.
/// This keeps DBAccess worker load minimal for trivial requests.
/// </summary>
public sealed class LocalBackendService : IBackendService
{
    private readonly DataDir _dir;

    public LocalBackendService(DataDir dir)
    {
        _dir = dir;
    }

    public Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken)
    {
        var header = request.Header ?? new RequestHeader();

        // RequestId=1 : fixed response (process list)
        var fixedList = new List<(int RowNo, string Value)>
        {
            (1, "1PC"),
            (2, "2PC"),
            (3, "アプリ連携"),
        };

        ResultStore.WriteAll(_dir.Path, resultId, fixedList);

        return Task.FromResult(new BuildReply
        {
            Header = header,
            ResultId = resultId,
            TotalCount = fixedList.Count,
            Message = "Ready (fixed list)"
        });
    }

    public Task CancelAsync(RequestState state, CancellationToken cancellationToken)
    {
        // No downstream cancellation for local backend.
        return Task.CompletedTask;
    }
}
