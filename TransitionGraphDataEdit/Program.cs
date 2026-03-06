using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using GlobalCache;
using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphDataEdit;
using TransitionGraphDataEdit.Backends;
using TransitionGraphDataEdit.Models;
using TransitionGraphDataEdit.Grid;
using System.Collections.Concurrent;
using TransitionGraph.PortSetting;
using System.Reflection;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

static (int port, string dataDir, string dbAccessAddress, string alignmentAddress) ParseArgs(string[] args)
{
    var ports = PortSettingStore.Current;
    int port = ports.WorkerDataEditPort;
    string dataDir = Path.Combine(AppContext.BaseDirectory, "data");
    string dbAccessAddress = ports.WorkerDbAccessAddress;
    string alignmentAddress = ports.WorkerAlignmentAddress;

    for (int i = 0; i < args.Length; i++)
    {
        if (args[i] == "--port" && i + 1 < args.Length && int.TryParse(args[i + 1], out var p))
            port = p;
        if (args[i] == "--dataDir" && i + 1 < args.Length)
            dataDir = args[i + 1];
        if (args[i] == "--dbAccessAddress" && i + 1 < args.Length)
            dbAccessAddress = args[i + 1];
        if (args[i] == "--alignmentAddress" && i + 1 < args.Length)
            alignmentAddress = args[i + 1];
    }
    return (port, dataDir, dbAccessAddress, alignmentAddress);
}

var (port, dataDir, dbAccessAddress, alignmentAddress) = ParseArgs(args);

var options = new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
};

var builder = WebApplication.CreateBuilder(options);

// =================================================================
Console.WriteLine("===== DEBUG =====");
Console.WriteLine($"BaseDir={AppContext.BaseDirectory}");
Console.WriteLine($"CWD={Directory.GetCurrentDirectory()}");
Console.WriteLine($"Env={builder.Environment.EnvironmentName}");
Console.WriteLine($"UseDummy={builder.Configuration["GridDll:UseDummy"]}");
Console.WriteLine($"ContentRoot={builder.Environment.ContentRootPath}");
Console.WriteLine($"AppSettingsExists={File.Exists(Path.Combine(builder.Environment.ContentRootPath, "appsettings.json"))}");
// =================================================================

//builder.Services.AddGrpc();

builder.Services.AddGrpc(o =>
{
    o.EnableDetailedErrors = true;
});

builder.Services.AddSingleton(new DataDir(dataDir));
builder.Services.AddSingleton(new WorkerAddresses(dbAccessAddress, alignmentAddress));
builder.Services.AddSingleton<RequestStateStore>();
builder.Services.AddSingleton<LocalBackendService>();
builder.Services.AddSingleton<DbAccessBackendService>();
builder.Services.AddSingleton<AlignmentBackendService>();
builder.Services.AddGridClient(builder.Configuration);
builder.Services.AddSingleton<IstarBackendService>();
builder.Services.AddSingleton<GridFilterSortBackendService>();
builder.Services.AddSingleton<BackendRouter>();

builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenLocalhost(port, lo => lo.Protocols = HttpProtocols.Http2);
});

var app = builder.Build();
app.MapGrpcService<HealthService>();
app.MapGrpcService<ControlService>();
app.MapGrpcService<DataEditService>();
app.MapGet("/", () => "TransitionGraphDataEdit (gRPC)");

app.Run();

public sealed record DataDir(string Path);
public sealed record WorkerAddresses(string DbAccessAddress, string AlignmentAddress);

public sealed class DataEditService : DataEdit.DataEditBase
{
    private readonly DataDir _dir;
    private readonly WorkerAddresses _addrs;
    private readonly RequestStateStore _state;
    private readonly BackendRouter _router;
    private readonly DbAccessBackendService _dbBackend;
    private readonly AlignmentBackendService _alignmentBackend;

    public DataEditService(
        DataDir dir,
        WorkerAddresses addrs,
        RequestStateStore state,
        BackendRouter router,
        DbAccessBackendService dbBackend,
        AlignmentBackendService alignmentBackend)
    {
        _dir = dir;
        _addrs = addrs;
        _state = state;
        _router = router;
        _dbBackend = dbBackend;
        _alignmentBackend = alignmentBackend;
    }

    public override async Task<BuildReply> BuildResultSet(BuildRequest request, ServerCallContext context)
    {
        //DXC.EngPF.Core.Grid.gri
        //GridServiceClient gridClient = new GridServiceClient("http://localhost:50052","",0);
        //gridClient.GetGridLotInfoAsync

        var header = request.Header ?? new RequestHeader();
        if (string.IsNullOrWhiteSpace(header.Guid))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "header.guid is required"));
        if (string.IsNullOrWhiteSpace(header.RequestId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "header.requestId is required"));

        // RequestId=100: �ʃV�X�e��(Grid)��ServerBaseUrl��DataEdit�v���Z�X���ɕێ����邾��
        if (string.Equals(header.RequestId, "100", StringComparison.OrdinalIgnoreCase))
        {
            // API������̓n�����ɍ��킹�ďE���iParm1 / serverBaseUrl �ǂ���ł�OK�j
            request.Parameters.TryGetValue("Parm1", out var raw);
            if (string.IsNullOrWhiteSpace(raw))
                request.Parameters.TryGetValue("serverBaseUrl", out raw);

            raw ??= "";

            Cache.Set("RequestId100.Parameter0", raw);

            Console.WriteLine($"[GridApi] RequestId=100 cached. raw='{raw}'");

            return new BuildReply
            {
                Header = header,
                ResultId = "",      // BuildRequest��ResultId�͖����B�ԐM�͋��OK
                TotalCount = 0,
                Message = "RequestId=100 accepted"
            };
        }

        // =========================================================
        // Session / 2nd cache id handling (RequestId 7-10)
        //
        // Spec:
        //  (3) When receiving RequestId 7-10, if sessionId (Parm4) and/or 2nd cache id (Parm2)
        //      exist, always save them to cache (if only one exists, save the one).
        //  (4) When using sessionId / 2nd cache id in RequestId 7-10 processing,
        //      always use the cached values (i.e., fill missing parameters from cache).
        //
        // Note: These are process-local caches (MemoryCache) and expire at end of day.
        // =========================================================
        if (header.RequestId is "7" or "10")
        {
            const string KeySession = "RequestId7to10.SessionId";
            const string KeySecond = "RequestId7to10.SecondCacheId";

            request.Parameters.TryGetValue("Parm4", out var pSession); // sessionId
            request.Parameters.TryGetValue("Parm2", out var pSecond);  // 2nd cache id

            if (!string.IsNullOrWhiteSpace(pSession)) Cache.Set(KeySession, pSession);
            if (!string.IsNullOrWhiteSpace(pSecond)) Cache.Set(KeySecond, pSecond);

            if (string.IsNullOrWhiteSpace(pSession))
            {
                var cached = Cache.Get<string>(KeySession);
                if (!string.IsNullOrWhiteSpace(cached))
                    request.Parameters["Parm4"] = cached;
            }

            if (string.IsNullOrWhiteSpace(pSecond))
            {
                var cached = Cache.Get<string>(KeySecond);
                if (!string.IsNullOrWhiteSpace(cached))
                    request.Parameters["Parm2"] = cached;
            }
        }

        
// =========================================================
// (NEW) Propagate cached SessionId / 2nd cache id to Alignment requests (RequestId 8/9)
// - RequestId 8/9 do not carry these IDs in Parm2/Parm4 per spec.
// - Alignment worker expects them as named keys (SessionId / SecondCacheId).
// =========================================================
if (header.RequestId is "8" or "9")
{
    const string KeySession = "RequestId7to10.SessionId";
    const string KeySecond = "RequestId7to10.SecondCacheId";

    // Only inject when missing or blank to avoid overriding explicit inputs.
    if (!request.Parameters.TryGetValue("SessionId", out var s) || string.IsNullOrWhiteSpace(s))
    {
        var cached = Cache.Get<string>(KeySession);
        if (!string.IsNullOrWhiteSpace(cached))
            request.Parameters["SessionId"] = cached;
    }

    if (!request.Parameters.TryGetValue("SecondCacheId", out var sc) || string.IsNullOrWhiteSpace(sc))
    {
        var cached = Cache.Get<string>(KeySecond);
        if (!string.IsNullOrWhiteSpace(cached))
            request.Parameters["SecondCacheId"] = cached;
    }

    // Also set legacy lower-camel for safety (Alignment builders read both).
    if (!request.Parameters.TryGetValue("secondCacheId", out var sc2) || string.IsNullOrWhiteSpace(sc2))
    {
        if (request.Parameters.TryGetValue("SecondCacheId", out var v) && !string.IsNullOrWhiteSpace(v))
            request.Parameters["secondCacheId"] = v;
    }
}

// Ensure requestInstanceId exists (required for cancel)
        if (string.IsNullOrWhiteSpace(header.RequestInstanceId))
            header.RequestInstanceId = Guid.NewGuid().ToString("D");

        // (10) Cache lookup for RequestId 4-6
        // If the same dataset request has already been persisted as ResultStore(csv), reuse it.
        // GlobalCache stores only metadata (resultId + totalCount).
        string? cacheKey = null;
        if (header.RequestId is "4" or "5" or "6")
        {
            // Spec (7): cache is keyed by "GUID + parameters" for RequestId 4-6.
            cacheKey = CacheKeyBuilder.BuildForDataSet(header.Guid, header.RequestId, request.Parameters);
            var cached = Cache.Get<CachedResultMeta>(cacheKey);
            if (cached is not null)
            {
                var path = ResultStore.GetPath(_dir.Path, cached.ResultId);
                if (File.Exists(path))
                {
                    return new BuildReply
                    {
                        Header = header,
                        ResultId = cached.ResultId,
                        TotalCount = cached.TotalCount,
                        Message = "Ready (cache hit)"
                    };
                }
            }
        }

        var resultId = Guid.NewGuid().ToString("D");

        // Register cancellation token for this request so it can be canceled by another RPC.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);

        // Choose backend based on RequestId (spec (5))
        var (backend, backendKind) = _router.Select(header.RequestId);

        // Alignment backend: pass through the DataEdit-generated resultId so that
        // result persistence and cancellation use the same ID across processes.
        if (backendKind == BackendKind.Alignment && !request.Parameters.ContainsKey("__resultId"))
        {
            request.Parameters["__resultId"] = resultId;
        }

        // (3-2) If RequestId=2 and process/time changed for the same GUID, treat previous as canceled.
        if (string.Equals(header.RequestId, "2", StringComparison.OrdinalIgnoreCase))
        {
            request.Parameters.TryGetValue("process", out var proc);
            request.Parameters.TryGetValue("startTime", out var st);
            request.Parameters.TryGetValue("endTime", out var et);
            var signature = $"{proc ?? ""}|{st ?? ""}|{et ?? ""}";

            if (_state.TryCancelPreviousForKey2Change(header.Guid, signature, out var prev))
            {
                await CleanupAsync(prev, cancellationToken: CancellationToken.None);
            }

            _state.Register(header, resultId, cts, signature, backendKind);
        }
        else
        {
            _state.Register(header, resultId, cts, signature: null, backendKind);
        }

        try
        {
            // Execute backend
            var reply = await backend.BuildAsync(request, resultId, cts.Token);

            // Some backends (Alignment) may generate their own resultId.
            // Update runtime state so Cancel cleans up correctly.
            if (!string.IsNullOrWhiteSpace(header.RequestInstanceId)
                && !string.IsNullOrWhiteSpace(reply.ResultId)
                && !string.Equals(reply.ResultId, resultId, StringComparison.OrdinalIgnoreCase))
            {
                _state.UpdateResultId(header.RequestInstanceId, reply.ResultId);
            }

            // (10) Cache save for RequestId 4-6
            if (cacheKey is not null)
            {
                if (!string.IsNullOrWhiteSpace(header.RequestInstanceId))
                    _state.AddCacheKey(header.RequestInstanceId, cacheKey);

                Cache.Set(cacheKey, new CachedResultMeta(reply.ResultId, reply.TotalCount));
            }

            return reply;
        }
        catch (OperationCanceledException)
        {
            // Best-effort cleanup on cancellation
            // IMPORTANT: use the stored state so we can remove all GlobalCache keys created by this request.
            if (_state.TryGet(header.RequestInstanceId, out var st))
            {
                await CleanupAsync(st, CancellationToken.None);
            }
            else
            {
                await CleanupAsync(new RequestState(
                    header.Guid,
                    header.RequestId,
                    header.RequestInstanceId,
                    resultId,
                    null,
                    null,
                    backendKind,
                    new ConcurrentBag<string>()), CancellationToken.None);
            }
            return new BuildReply
            {
                Header = header,
                ResultId = resultId,
                TotalCount = 0,
                Message = "Canceled"
            };
        }
        catch (Exception ex)
        {
            // IMPORTANT:
            // Unhandled exceptions in the handler become StatusCode=Unknown on the client.
            // Log full details here and return a meaningful status to the caller.
            Console.WriteLine("=== DataEdit BuildResultSet Unhandled Exception ===");
            Console.WriteLine(ex.ToString());

            // Unwrap common wrapper exceptions to surface the real root cause to callers.
            var rootEx = ex;
            if (rootEx is TargetInvocationException tie && tie.InnerException is not null)
                rootEx = tie.InnerException;
            if (rootEx is AggregateException ae && ae.InnerExceptions.Count == 1)
                rootEx = ae.InnerExceptions[0];


            // Surface the exception type + message to the API layer (keep it short).
            //throw new RpcException(new Status(StatusCode.Internal, $"{rootEx.GetType().Name}: {rootEx.Message}"));
            var detail = rootEx.ToString();
            if (detail.Length > 3500) detail = detail.Substring(0, 3500); // gRPC��Detail�͒��߂���ƈ����Â炢�̂ŃJ�b�g

            throw new RpcException(new Status(StatusCode.Internal, detail));
        }
        finally
        {
            _state.Complete(header.RequestInstanceId);
        }
    }

    public override async Task<CancelReply> Cancel(CancelRequest request, ServerCallContext context)
    {
        var header = request.Header ?? new RequestHeader();
        if (string.IsNullOrWhiteSpace(header.Guid) || string.IsNullOrWhiteSpace(header.RequestId))
        {
            return new CancelReply { Canceled = false, Message = "header.guid and header.requestId are required" };
        }

        // Prefer requestInstanceId if provided; otherwise cancel by (clientGuid, requestKey)
        if (_state.TryCancel(header, request.ResultId ?? string.Empty, out var state))
        {
            await CleanupAsync(state, CancellationToken.None);
            // Best-effort: remove runtime state immediately
            _state.Complete(state.RequestInstanceId);
            return new CancelReply { Canceled = true, Message = "Canceled" };
        }

        return new CancelReply { Canceled = false, Message = "NotFound" };
    }

    private async Task CleanupAsync(RequestState state, CancellationToken cancellationToken)
    {
        // Downstream cancellation
        try
        {
            switch (state.Backend)
            {
                case BackendKind.Alignment:
                    await _alignmentBackend.CancelAsync(state, cancellationToken);
                    break;
                case BackendKind.DbAccess:
                    await _dbBackend.CancelAsync(state, cancellationToken);
                    break;
                case BackendKind.Local:
                default:
                    break;
            }
        }
        catch
        {
            // best-effort
        }

        // Delete ResultStore file (acts as cache in current phase)
        try
        {
            if (!string.IsNullOrWhiteSpace(state.ResultId))
            {
                var path = ResultStore.GetPath(_dir.Path, state.ResultId);
                if (File.Exists(path))
                    File.Delete(path);
                var tmp = path + ".tmp";
                if (File.Exists(tmp))
                    File.Delete(tmp);
            }
        }
        catch
        {
            // best-effort
        }

        // (3-3) Remove GlobalCache entries created by this request (phase 10 metadata cache)
        try
        {
            if (state.CacheKeys is not null)
            {
                foreach (var k in state.CacheKeys)
                {
                    if (!string.IsNullOrWhiteSpace(k))
                        Cache.Remove(k);
                }
            }
        }
        catch
        {
            // best-effort
        }
    }
}
