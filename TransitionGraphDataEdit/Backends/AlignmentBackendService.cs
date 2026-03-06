using Grpc.Net.Client;
using System.Net.Http;
using TransitionGraph.V1;
using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// Backend implementation forwarding to TransitionGraphAlignment service.
///
/// This is used for RequestId 7-9.
/// </summary>
public sealed class AlignmentBackendService : IBackendService
{
    private readonly WorkerAddresses _addrs;

    public AlignmentBackendService(WorkerAddresses addrs)
    {
        _addrs = addrs;
    }

    public async Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken)
    {
        // Forward as-is. Alignment will generate/persist its own result.
        using var alCh = GrpcChannel.ForAddress(_addrs.AlignmentAddress, new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true }
        });
        var alClient = new Alignment.AlignmentClient(alCh);

        return await alClient.BuildResultSetAsync(request, cancellationToken: cancellationToken);
    }

    public async Task CancelAsync(RequestState state, CancellationToken cancellationToken)
    {
        try
        {
            using var alCh = GrpcChannel.ForAddress(_addrs.AlignmentAddress, new GrpcChannelOptions
            {
                HttpHandler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true }
            });
            var alClient = new Alignment.AlignmentClient(alCh);
            await alClient.CancelAsync(new CancelRequest
            {
                Header = new RequestHeader
                {
                    Guid = state.Guid,
                    RequestId = state.RequestId,
                    RequestInstanceId = state.RequestInstanceId
                },
                ResultId = state.ResultId ?? string.Empty
            }, cancellationToken: cancellationToken);
        }
        catch
        {
            // best-effort
        }
    }
}
