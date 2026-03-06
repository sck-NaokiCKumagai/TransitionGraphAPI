using Grpc.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using TransitionGraph.Db;
using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraph.PortSetting;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// ============================================================
//  TransitionGraphDBAccess (gRPC)
//  - DB queries are executed on-demand in FetchPage / FetchAll.
//  - This process should "start" even if DB is unreachable.
//  - To help troubleshooting, we write startup/error logs under dataDir.
// ============================================================

static (int port, string dataDir) ParseArgs(string[] args)
{
    int port = PortSettingStore.Current.WorkerDbAccessPort;
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

static void SafeAppend(string filePath, string line)
{
    try
    {
        Directory.CreateDirectory(Path.GetDirectoryName(filePath) ?? ".");
        File.AppendAllText(filePath, line + "\n");
    }
    catch
    {
        // do nothing (logging must not crash startup)
    }
}

var (port, dataDir) = ParseArgs(args);

// 念のため末尾スラッシュを落として正規化
dataDir = Path.GetFullPath(dataDir.TrimEnd('\\', '/'));
Directory.CreateDirectory(dataDir);

var bootLog = Path.Combine(dataDir, "dbaccess_boot.log");
var errLog = Path.Combine(dataDir, "dbaccess_error.log");

SafeAppend(bootLog, $"{DateTime.Now:O} BOOT pid={Environment.ProcessId} port={port} dataDir={dataDir} baseDir={AppContext.BaseDirectory}");

// Capture unexpected exceptions into a file as well
AppDomain.CurrentDomain.UnhandledException += (_, e) =>
{
    SafeAppend(errLog, $"{DateTime.Now:O} UNHANDLED {e.ExceptionObject}");
};
TaskScheduler.UnobservedTaskException += (_, e) =>
{
    SafeAppend(errLog, $"{DateTime.Now:O} UNOBSERVED {e.Exception}");
    e.SetObserved();
};

var builder = WebApplication.CreateBuilder(args);

// Load configuration defensively:
// - Running as "dotnet run" : content root has appsettings.json
// - Running as published exe : BaseDirectory has appsettings.json
builder.Configuration
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

var baseAppSettings = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
if (File.Exists(baseAppSettings))
{
    builder.Configuration.AddJsonFile(baseAppSettings, optional: true, reloadOnChange: true);
}

// Environment variables (TG_ prefix only)
builder.Configuration.AddEnvironmentVariables(prefix: "TG_");

builder.Services.AddGrpc(o =>
{
    // Helps debugging by preserving exception details across gRPC boundaries.
    o.EnableDetailedErrors = true;
});

// DB layer
builder.Services.Configure<DatabaseOptions>(builder.Configuration.GetSection("Database"));
builder.Services.AddSingleton<IDbConnectionFactory, DbConnectionFactory>();

// OpenQuery で受け取った (sqlKey, parameters) を resultId ごとに保持
builder.Services.AddSingleton<IQueryRegistry, InMemoryQueryRegistry>();

// Seek paging cursor store (uses lastRowNo as opaque cursorId)
builder.Services.AddSingleton<ISeekCursorStore, InMemorySeekCursorStore>();
// SQL定型文カタログ（Json）

builder.Services.AddSingleton<ISqlCatalog>(sp =>
{
    var opt = sp.GetRequiredService<IOptions<DatabaseOptions>>().Value;
    var catalogPath = Path.Combine(AppContext.BaseDirectory, "SqlCatalog.json");
    return new JsonSqlCatalog(catalogPath, opt.Provider);
});

builder.Services.AddSingleton<ISqlDialect>(sp =>
{
    var opt = sp.GetRequiredService<IOptions<DatabaseOptions>>().Value;
    return opt.Provider.Equals("Vertica", StringComparison.OrdinalIgnoreCase)
        ? new VerticaDialect()
        : new SqlServerDialect();
});

builder.Services.AddSingleton<IResultSetRepository>(sp =>
{
    var factory = sp.GetRequiredService<IDbConnectionFactory>();
    var dialect = sp.GetRequiredService<ISqlDialect>();
    var registry = sp.GetRequiredService<IQueryRegistry>();
    var catalog = sp.GetRequiredService<ISqlCatalog>();
    var seekStore = sp.GetRequiredService<ISeekCursorStore>();
    return new ResultSetRepository(factory, dialect, registry, catalog, seekStore, dataDir);
});

builder.WebHost.ConfigureKestrel(o =>
{
    // gRPC only
    o.ListenLocalhost(port, lo => lo.Protocols = HttpProtocols.Http2);
});

var app = builder.Build();

// DI後の値（IOptions）で確認ログを出す
app.Lifetime.ApplicationStarted.Register(() =>
{
    try
    {
        using var scope = app.Services.CreateScope();
        var opt = scope.ServiceProvider
            .GetRequiredService<IOptions<DatabaseOptions>>()
            .Value;

        var csLen = opt.Provider.Equals("Vertica", StringComparison.OrdinalIgnoreCase)
            ? (opt.Vertica.ConnectionString?.Length ?? -1)
            : (opt.SqlServer.ConnectionString?.Length ?? -1);

        SafeAppend(bootLog, $"{DateTime.Now:O} STARTED provider={opt.Provider} csLen={csLen}");
    }
    catch (Exception ex)
    {
        SafeAppend(errLog, $"{DateTime.Now:O} ERROR started-hook {ex}");
    }
});

app.MapGrpcService<HealthService>();
app.MapGrpcService<ControlService>();
app.MapGrpcService<DBAccessGrpcService>();
app.MapGet("/", () => "TransitionGraphDBAccess (gRPC)");

try
{
    app.Run();
}
catch (Exception ex)
{
    SafeAppend(errLog, $"{DateTime.Now:O} FATAL {ex}");
    throw;
}

public sealed class DBAccessGrpcService : DBAccess.DBAccessBase
{
    private readonly IResultSetRepository _resultRepo;
    private readonly IQueryRegistry _registry;

    public DBAccessGrpcService(IResultSetRepository resultRepo, IQueryRegistry registry)
    {
        _resultRepo = resultRepo;
        _registry = registry;
    }

    public override Task<OpenQueryReply> OpenQuery(OpenQueryRequest request, ServerCallContext context)
    {
        // Keep the query context in-memory (resultId -> sqlKey, parameters)
        var rid = Guid.Parse(request.ResultId);
        var sqlKey = request.SqlKey ?? "";

        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in request.Parameters)
            dict[kv.Key] = kv.Value;

        _registry.Register(new QueryContext(rid, sqlKey, dict));
        return Task.FromResult(new OpenQueryReply { Message = "OK" });
    }

    public override Task<CancelQueryReply> CancelQuery(CancelQueryRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ResultId, out var rid))
        {
            return Task.FromResult(new CancelQueryReply
            {
                Removed = false,
                Message = "Invalid resultId"
            });
        }

        var removed = _registry.Remove(rid);
        return Task.FromResult(new CancelQueryReply
        {
            Removed = removed,
            Message = removed ? "Removed" : "NotFound"
        });
    }

    public override async Task<PageReply> FetchPage(FetchPageRequest request, ServerCallContext context)
    {
        try
        {
            var result = await _resultRepo.FetchPageAsync(
                Guid.Parse(request.ResultId),
                request.LastRowNo,
                request.PageSize,
                context.CancellationToken);

            var reply = new PageReply
            {
                NextLastRowNo = result.NextLastRowNo,
                Done = result.Done,
                ReturnedCount = result.Items.Count
            };

            foreach (var it in result.Items)
                reply.Items.Add(new Point { RowNo = it.RowNo, Value = it.Value });

            return reply;
        }
        catch (Exception ex)
        {
            // Emit the real underlying exception so the caller can debug.
            // Write to both STDOUT and STDERR so logs are visible even if only one stream is captured.
            Console.WriteLine("=== DBAccess FetchPage Unhandled Exception ===");
            Console.WriteLine(ex.ToString());
            Console.WriteLine("==========================================");
            Console.Error.WriteLine("=== DBAccess FetchPage Unhandled Exception ===");
            Console.Error.WriteLine(ex.ToString());
            Console.Error.WriteLine("==========================================");

            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override async Task<ProbeItemConvertMasterReply> ProbeItemConvertMaster(ProbeItemConvertMasterRequest request, ServerCallContext context)
    {
        // The old probe was for ITEM_CONVERT_MASTER. That table may not exist.
        // Keep the RPC for compatibility but do not query DB here.
        return await Task.FromResult(new ProbeItemConvertMasterReply
        {
            RowsFetched = 0,
            Message = "Deprecated. Use FetchPage API."
        });
    }
}
