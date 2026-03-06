using System.Runtime.Loader;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using System.Collections.Concurrent;
using System.Text.Json;
using GlobalCache;
using TransitionGraph.Dtos;
using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphAlignment;
using TransitionGraphAlignment.Grid;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

InstallDxcDllResolver();

static void InstallDxcDllResolver()
{
    // Find root\DLL (contains DXC.EngPF.Core.Grid.dll and its dependencies such as DXC.EngPF.Framework.Log.dll).
    var baseDir = AppContext.BaseDirectory;
    string? dllDir = null;
    var cur = new DirectoryInfo(baseDir);
    for (int i = 0; i < 8 && cur != null; i++)
    {
        var cand = Path.Combine(cur.FullName, "DLL");
        if (File.Exists(Path.Combine(cand, "DXC.EngPF.Core.Grid.dll"))) { dllDir = cand; break; }
        // also check sibling DLL under repo root when baseDir is ...\TransitionGraphAlignment\bin\...\net8.0
        var parent = cur.Parent;
        if (parent != null)
        {
            var sib = Path.Combine(parent.FullName, "DLL");
            if (File.Exists(Path.Combine(sib, "DXC.EngPF.Core.Grid.dll"))) { dllDir = sib; break; }
        }
        cur = cur.Parent;
    }

    if (string.IsNullOrWhiteSpace(dllDir))
        return;

    AssemblyLoadContext.Default.Resolving += (context, name) =>
    {
        try
        {
            var path = Path.Combine(dllDir!, name.Name + ".dll");
            if (File.Exists(path))
                return context.LoadFromAssemblyPath(path);
        }
        catch
        {
            // best-effort
        }
        return null;
    };
}

static (int port, string dataDir) ParseArgs(string[] args)
{
    int port = 5104;
    string dataDir = Path.Combine(AppContext.BaseDirectory, "data");

    for (int i = 0; i < args.Length; i++)
    {
        if (args[i] == "--port" && i + 1 < args.Length && int.TryParse(args[i + 1], out var p))
            port = p;
        if (args[i] == "--dataDir" && i + 1 < args.Length)
            dataDir = args[i + 1];
    }
    return (port, dataDir);
}

var (port, dataDir) = ParseArgs(args);

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

builder.Services.AddGrpc();
builder.Services.AddSingleton(new DataDir(dataDir));
builder.Services.AddSingleton<RequestStateStore>();

// Switchable Grid client (Real/Dummy) by configuration (appsettings.json):
// GridDll:
//   UseDummy: true/false
//   DummyCsvDir: (optional)
builder.Services.AddGridClient(builder.Configuration, dataDir);
builder.Services.AddSingleton<Request8Builder>();
builder.Services.AddSingleton<Request9Builder>();

builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenLocalhost(port, lo => lo.Protocols = HttpProtocols.Http2);
});

var app = builder.Build();
app.MapGrpcService<HealthService>();
app.MapGrpcService<ControlService>();
app.MapGrpcService<AlignmentService>();
app.MapGet("/", () => "TransitionGraphAlignment (gRPC)");

app.Run();

public sealed record DataDir(string Path);

/// <summary>
/// Minimal state store for cancel propagation.
/// Alignment will later host long-running processing; this registry
/// enables best-effort cancellation by requestInstanceId.
/// </summary>
public sealed class RequestStateStore
{
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _byInstanceId = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _resultIdByInstanceId = new(StringComparer.OrdinalIgnoreCase);

    public void Register(string requestInstanceId, string resultId, CancellationTokenSource cts)
    {
        if (string.IsNullOrWhiteSpace(requestInstanceId))
            return;
        _byInstanceId[requestInstanceId] = cts;
        _resultIdByInstanceId[requestInstanceId] = resultId;
    }

    public bool TryCancel(string requestInstanceId, out string? resultId)
    {
        resultId = null;
        if (string.IsNullOrWhiteSpace(requestInstanceId))
            return false;
        if (_byInstanceId.TryGetValue(requestInstanceId, out var cts))
        {
            cts.Cancel();
            _resultIdByInstanceId.TryGetValue(requestInstanceId, out resultId);
            return true;
        }
        return false;
    }

    public void Complete(string requestInstanceId)
    {
        if (string.IsNullOrWhiteSpace(requestInstanceId))
            return;
        _byInstanceId.TryRemove(requestInstanceId, out _);
        _resultIdByInstanceId.TryRemove(requestInstanceId, out _);
    }
}

/// <summary>
/// Placeholder Alignment worker.
/// Current phase: supports routing for RequestId 7-9.
/// </summary>
public sealed class AlignmentService : Alignment.AlignmentBase
{
    private readonly DataDir _dir;
    private readonly RequestStateStore _state;
    private readonly Request8Builder _req8;
    private readonly Request9Builder _req9;

    public AlignmentService(DataDir dir, RequestStateStore state, Request8Builder req8, Request9Builder req9)
    {
        _dir = dir;
        _state = state;
        _req8 = req8;
        _req9 = req9;
    }

    public override Task<BuildReply> BuildResultSet(BuildRequest request, ServerCallContext context)
    {
        var header = request.Header ?? new RequestHeader();

        if (string.IsNullOrWhiteSpace(header.Guid))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "header.guid is required"));
        if (string.IsNullOrWhiteSpace(header.RequestId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "header.requestId is required"));

        // Ensure requestInstanceId exists (required for cancel)
        if (string.IsNullOrWhiteSpace(header.RequestInstanceId))
            header.RequestInstanceId = Guid.NewGuid().ToString("D");

        // DataEdit may pass __resultId so that persistence/cancel uses the same ID.
        var resultId = request.Parameters.TryGetValue("__resultId", out var rid) && !string.IsNullOrWhiteSpace(rid)
            ? rid
            : Guid.NewGuid().ToString("D");

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        _state.Register(header.RequestInstanceId, resultId, cts);
        string msg;

        try
        {

            // ----------------------------------------------------------
            // Cache SessionId / SecondCacheId coming from DataEdit (Req 8/9)
            // ----------------------------------------------------------
            if (header.RequestId is "8" or "9")
            {
                const string KeySession = "RequestId7to10.SessionId";
                const string KeySecond = "RequestId7to10.SecondCacheId";

                request.Parameters.TryGetValue("SessionId", out var sessionId);
                request.Parameters.TryGetValue("SecondCacheId", out var secondCacheId);

                if (string.IsNullOrWhiteSpace(sessionId))
                    request.Parameters.TryGetValue("sessionId", out sessionId);
                if (string.IsNullOrWhiteSpace(secondCacheId))
                    request.Parameters.TryGetValue("secondCacheId", out secondCacheId);

                if (!string.IsNullOrWhiteSpace(sessionId))
                    GlobalCache.Cache.Set(KeySession, sessionId);
                if (!string.IsNullOrWhiteSpace(secondCacheId))
                    GlobalCache.Cache.Set(KeySecond, secondCacheId);
            }

            // RequestId=100: store the other-system server base address in a process-wide cache.
            // This enables RequestId=8/9 (Alignment worker) to use the same server address as RequestId=7/10.
            if (header.RequestId == "100")
            {
                // API forwards the server address as Parm1.
                request.Parameters.TryGetValue("Parm1", out var raw);
                raw ??= string.Empty;

                // Keep the key consistent with DataEdit for unification.
                GlobalCache.Cache.Set("RequestId100.Parameter0", raw);

                msg = "OK";
                ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, "RequestId=100 accepted") });
            }
            else if (header.RequestId == "8")
            {
                // Build GraphDataDto and persist as a single JSON line.
                var dto = _req8.BuildAsync(header.Guid, request.Parameters, cts.Token).GetAwaiter().GetResult();
                var json = JsonSerializer.Serialize(dto);
                msg = "OK";
                ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, json) });
            }
            else if (header.RequestId == "9")
            {
                // Build GraphDataDto(RequestId=9) and persist as a single JSON line.
                var dto = _req9.BuildAsync(header.Guid, request.Parameters, cts.Token).GetAwaiter().GetResult();
                var json = JsonSerializer.Serialize(dto);
                msg = "OK";
                ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, json) });
            }
            else
            {
                msg = $"Alignment is not implemented yet. RequestId={header.RequestId}";
                ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, msg) });
            }
        }
        finally
        {
            _state.Complete(header.RequestInstanceId);
        }

        return Task.FromResult(new BuildReply
        {
            Header = header,
            ResultId = resultId,
            TotalCount = 1,
            Message = msg
        });
    }

    public override Task<CancelReply> Cancel(CancelRequest request, ServerCallContext context)
    {
        var header = request.Header ?? new RequestHeader();
        var resultId = request.ResultId ?? string.Empty;

        if (!string.IsNullOrWhiteSpace(header.RequestInstanceId) && _state.TryCancel(header.RequestInstanceId, out var storedResultId))
        {
            resultId = string.IsNullOrWhiteSpace(resultId) ? (storedResultId ?? "") : resultId;
        }

        // Best-effort: delete persisted result file if it exists.
        try
        {
            if (!string.IsNullOrWhiteSpace(resultId))
            {
                var path = ResultStore.GetPath(_dir.Path, resultId);
                if (File.Exists(path))
                    File.Delete(path);
                var tmp = path + ".tmp";
                if (File.Exists(tmp))
                    File.Delete(tmp);
            }
        }
        catch
        {
            // ignore
        }

        return Task.FromResult(new CancelReply { Canceled = true, Message = "Canceled (alignment)" });
    }
}

