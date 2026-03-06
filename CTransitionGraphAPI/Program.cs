using System.Diagnostics;
using System.Net;
using System.Linq;
using System.Text.Json;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using TransitionGraph.Dtos;
using TransitionGraph.LogOutPut;
using TransitionGraphDtos.Wpf;
using TransitionGraph.PortSetting;

// -------------------------------
// CTransitionGraphAPI
//  - WPF -> (this) -> Server API relay
//  - WPF -> (this) -> Output (RequestId=11)
// -------------------------------

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

var ports = PortSettingStore.Current;
var port = builder.Configuration.GetValue<int?>("App:ListenPort") ?? ports.ApiClientPort;

// Allow passing --port 5000
for (int i = 0; i + 1 < args.Length; i++)
{
    if (args[i].Equals("--port", StringComparison.OrdinalIgnoreCase) && int.TryParse(args[i + 1], out var p))
        port = p;
}

// IMPORTANT:
// Do not hard-bind Kestrel here, so that standard hosting configuration works:
// - command line: --urls http://127.0.0.1:5500
// - env: ASPNETCORE_URLS
// If neither is provided, fall back to App:ListenPort / --port.
var hasUrlsArg = args.Any(a => a.Equals("--urls", StringComparison.OrdinalIgnoreCase)) ||
                 args.Any(a => a.StartsWith("--urls=", StringComparison.OrdinalIgnoreCase));
var hasEnvUrls = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ASPNETCORE_URLS"));

if (!hasUrlsArg && !hasEnvUrls)
{
    builder.WebHost.UseUrls($"{ports.Scheme}://{ports.Host}:{port}");
}
builder.Services.AddResponseCompression();

builder.Services.AddSingleton<ServerEndpointStore>();
builder.Services.AddSingleton<OutputProcessManager>();

var app = builder.Build();
app.UseResponseCompression();

var endpointStore = app.Services.GetRequiredService<ServerEndpointStore>();
var outputMgr = app.Services.GetRequiredService<OutputProcessManager>();

// Start Output worker (best-effort)
await outputMgr.StartAsync(app.Configuration);

// ---------------------------------
// Config: how to extract server URL
// ---------------------------------
var cfgReqId = app.Configuration.GetValue<string>("ServerConfig:RequestId") ?? "";
var cfgParamIndex = app.Configuration.GetValue<int?>("ServerConfig:ParameterIndex") ?? 0;
var cfgAlsoAcceptIfLooksLikeUrl = app.Configuration.GetValue<bool?>("ServerConfig:AcceptAnyRequestIfParamLooksLikeUrl") ?? true;

static async Task LogHttpResponseAsync(ILogger logger, HttpResponseMessage resp, string tag)
{
    var body = await resp.Content.ReadAsStringAsync();
    //MessageOut.MessageOutPut("{Tag} HTTP {StatusCode} {Reason}", tag, (int)resp.StatusCode, resp.ReasonPhrase);

    // 長すぎる場合はカット
    if (body.Length > 4000) body = body.Substring(0, 4000) + "...(truncated)";
    MessageOut.MessageOutPut("{Tag} Body: {Body}", tag, body);
}

static bool TryNormalizeServerBase(string raw, string defaultScheme, int defaultPort, out string normalized)
{
    normalized = "";
    if (string.IsNullOrWhiteSpace(raw)) return false;

    raw = raw.Trim();
    var originalRaw = raw;

    // If the input is only "host" (or "ip"), append the default port.
    // Spec(RequestId=101): parameter[1] is a server API IP/host (no scheme/port).
    // Keep backward compatibility: if caller already provides host:port or URL, do nothing.
    if (!originalRaw.Contains("://", StringComparison.OrdinalIgnoreCase) &&
        !originalRaw.Contains(':') &&
        defaultPort > 0)
    {
        raw = originalRaw + ":" + defaultPort;
    }

    // Accept "host:port" and add scheme.
    if (!raw.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
        !raw.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
    {
        defaultScheme = string.IsNullOrWhiteSpace(defaultScheme) ? "http" : defaultScheme.Trim();
        defaultScheme = defaultScheme.TrimEnd(':', '/');
        raw = defaultScheme + "://" + raw;
    }

    if (!Uri.TryCreate(raw, UriKind.Absolute, out var uri)) return false;
    if (uri.Scheme is not ("http" or "https")) return false;

    // Keep scheme://host:port (no path)
    var b = new UriBuilder(uri) { Path = "", Query = "", Fragment = "" };

    // If port is not explicitly specified and we have a defaultPort (e.g., 5000), apply it.
    // (UriBuilder.Port can be -1 when omitted; some runtimes may surface 80/443 for default ports.)
    if (defaultPort > 0 && (b.Port is -1 or 0 || uri.IsDefaultPort))
    {
        // Avoid forcing when the defaultPort is the standard port for the scheme.
        var std = b.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? 443 : 80;
        if (defaultPort != std)
        {
            b.Port = defaultPort;
        }
    }

    normalized = b.Uri.ToString().TrimEnd('/');
    return true;
}

static string? FindServerBaseInParameters(List<string>? parms, string defaultScheme, int defaultPort)
{
    if (parms is null) return null;
    foreach (var s in parms)
    {
        if (TryNormalizeServerBase(s, defaultScheme, defaultPort, out var n)) return n;
    }
    return null;
}

// ---------------------------------
// Endpoints (mirror server)
// ---------------------------------

//app.MapPost("/api/transitiongraph/query", async (HttpContext ctx, RequestHeaderDto header) =>
//app.MapPost("/api/transitiongraph/query", async (HttpContext ctx, RequestHeaderDto header, ILogger<Program> logger) =>
app.MapPost("/api/transitiongraph/query", async (HttpContext ctx, WpfQueryDto req, ILogger<Program> logger) =>
{
    var header = new RequestHeaderDto
    {
        // IMPORTANT:
        // WPF request uses "guid" field, while the server expects "GUID" in RequestHeaderDto.
        // If GUID is missing, TransitionGraphDataEdit rejects the request (header.guid is required).
        GUID = req.guid ?? string.Empty,
        RequestId = req.requestId ?? string.Empty,
        Parameter = req.parameters ?? new List<string>(),
        RequestResult = RequestResultStatus.Running
    };

    // Backward compatibility / safety:
    // If guid field is not provided but parameter[0] looks like a GUID, use it as GUID.
    if (string.IsNullOrWhiteSpace(header.GUID))
    {
        var p0 = header.Parameter.ElementAtOrDefault(0);
        if (!string.IsNullOrWhiteSpace(p0) && Guid.TryParse(p0, out var _))
        {
            header.GUID = p0;
        }
    }

    MessageOut.MessageOutPut("CTransitionGraphAPI: /query received");
    MessageOut.MessageOutPut("CTransitionGraphAPI: /query received requestId={Id}", header?.RequestId);

    // (4) Server address configuration request
    //
    // In this repository, the interactive client/batch uses RequestId="SERVER_ADDR" to set
    // the downstream server base URL. This request must be handled locally (NO relay), because
    // the server-side API may reject requests with empty GUID and/or unknown RequestId.
    //
    // Also supports configurable RequestId + ParameterIndex via appsettings.
    //
    // Additionally supports RequestId="101" (spec): parameters = [GUID, ServerIP]
    // - If parameter[1] is an IP (or host), it will be normalized into a base URL using configured scheme and ApiServerPort.
    // - Always returns "Accept Request" as answer.
    var isServerAddrRequest = header?.RequestId == "SERVER_ADDR" ||
                              header?.RequestId == "101" ||
                              (!string.IsNullOrWhiteSpace(cfgReqId) && header?.RequestId == cfgReqId);

    if (isServerAddrRequest)
    {
        // If RequestId is explicitly configured, honor ParameterIndex.
        // Spec(RequestId=101): use parameter[1].
        // Default(SERVER_ADDR): use parameter[0].
        var paramIndex = (!string.IsNullOrWhiteSpace(cfgReqId) && header?.RequestId == cfgReqId)
            ? cfgParamIndex
            : (header?.RequestId == "101" ? 1 : 0);

        var raw = header.Parameter.ElementAtOrDefault(paramIndex) ?? "";
        if (TryNormalizeServerBase(raw, ports.Scheme, ports.ApiServerPort, out var normalized))
        {
            endpointStore.Set(normalized);
            var rid = header?.RequestId ?? "";
            MessageOut.MessageOutPut($"CTransitionGraphAPI: ServerBase updated by RequestId={rid}: {normalized}");
            header.RequestResult = RequestResultStatus.OK;
        }
        else
        {
            // Treat invalid input as NG but still return an empty response to keep WPF flow.
            header.RequestResult = RequestResultStatus.NG;
        }

        // 設定リクエスト自体はWPF側の流れを止めないためローカル応答で返す。
        // RequestId=101 は仕様上 "Accept Request" 固定。
        return Results.Ok(new ResponseDto<RequestAnwerDto>
        {
            Header = header,
            Answer = new RequestAnwerDto
            {
                RequestAnswer = header?.RequestId == "101" ? "Accept Request" : new { }
            }
        });
    }
    else if (cfgAlsoAcceptIfLooksLikeUrl)
    {
        // Fallback: if any parameter looks like an URL, treat as server base (only if not set yet)
        if (!endpointStore.HasValue)
        {
            var found = FindServerBaseInParameters(header?.Parameter, ports.Scheme, ports.ApiServerPort);
            if (!string.IsNullOrWhiteSpace(found))
            {
                endpointStore.Set(found);
                MessageOut.MessageOutPut($"CTransitionGraphAPI: ServerBase auto-detected: {found}");
            }
        }
    }

    // (5) Output: RequestId=11 -> send to Output worker, no answer required.
    if (header?.RequestId == "11")
    {
        await outputMgr.SendAsync(header, ctx.RequestAborted);
        // WPF互換のため、OK(空回答)で返す
        header.RequestResult = RequestResultStatus.OK;
        return Results.Ok(new ResponseDto<RequestAnwerDto>
        {
            Header = header,
            Answer = new RequestAnwerDto { RequestAnswer = new { } }
        });
    }

    // Relay to server
    var serverBase = endpointStore.GetOrDefault(app.Configuration.GetValue<string>("Server:BaseUrl") ?? PortSettingStore.Current.ApiServerBaseUrl);
    var targetUrl = serverBase.TrimEnd('/') + "/api/transitiongraph/query";

    MessageOut.MessageOutPut("Forwarding to server: {Url}", targetUrl);

    using var http = new HttpClient(new SocketsHttpHandler
    {
        AutomaticDecompression = DecompressionMethods.All
    })
    {
        Timeout = TimeSpan.FromMinutes(30)
    };

    var json = JsonSerializer.Serialize(header);
    
    MessageOut.MessageOutPut("Forward body: {Body}", json.Length > 2000 ? json[..2000] + "...(truncated)" : json);

    using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

    HttpResponseMessage resp;
    try
    {
        resp = await http.PostAsync(targetUrl, content, ctx.RequestAborted);

        MessageOut.MessageOutPut("Server status: {Status}", (LogKind)(int)resp.StatusCode);
    }
    catch (Exception ex)
    {
        MessageOut.MessageOutPut($"CTransitionGraphAPI: Relay failed: {ex.Message}");
        
        return Results.Problem($"Relay failed: {ex.Message}");
    }

    var body = await resp.Content.ReadAsStringAsync(ctx.RequestAborted);

    MessageOut.MessageOutPut("Server body: {Body}", body.Length > 4000 ? body[..4000] + "...(truncated)" : body);

    return Results.Text(body, "application/json", statusCode: (int)resp.StatusCode);
});

app.MapPost("/api/transitiongraph/cancel", async (HttpContext ctx, CancelRequestDto req) =>
{
    var serverBase = endpointStore.GetOrDefault(app.Configuration.GetValue<string>("Server:BaseUrl") ?? PortSettingStore.Current.ApiServerBaseUrl);
    var targetUrl = serverBase.TrimEnd('/') + "/api/transitiongraph/cancel";

    using var http = new HttpClient(new SocketsHttpHandler { AutomaticDecompression = DecompressionMethods.All })
    {
        Timeout = TimeSpan.FromMinutes(5)
    };

    var json = JsonSerializer.Serialize(req);
    using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

    HttpResponseMessage resp;
    try
    {
        resp = await http.PostAsync(targetUrl, content, ctx.RequestAborted);
    }
    catch (Exception ex)
    {
        MessageOut.MessageOutPut($"CTransitionGraphAPI: cancel relay failed: {ex.Message}");
        return Results.Problem($"Relay failed: {ex.Message}");
    }

    var body = await resp.Content.ReadAsStringAsync(ctx.RequestAborted);
    return Results.Text(body, "application/json", statusCode: (int)resp.StatusCode);
});

app.MapGet("/api/transitiongraph/result/{resultId}", async (HttpContext ctx, string resultId, string? cursor) =>
{
    var serverBase = endpointStore.GetOrDefault(app.Configuration.GetValue<string>("Server:BaseUrl") ?? PortSettingStore.Current.ApiServerBaseUrl);

    var url = serverBase.TrimEnd('/') + $"/api/transitiongraph/result/{WebUtility.UrlEncode(resultId)}";
    if (!string.IsNullOrWhiteSpace(cursor))
        url += "?cursor=" + WebUtility.UrlEncode(cursor);

    using var http = new HttpClient(new SocketsHttpHandler { AutomaticDecompression = DecompressionMethods.All })
    {
        Timeout = TimeSpan.FromMinutes(5)
    };

    HttpResponseMessage resp;
    try
    {
        resp = await http.GetAsync(url, ctx.RequestAborted);
    }
    catch (Exception ex)
    {
        MessageOut.MessageOutPut($"CTransitionGraphAPI: result relay failed: {ex.Message}");
        return Results.Problem($"Relay failed: {ex.Message}");
    }

    var body = await resp.Content.ReadAsStringAsync(ctx.RequestAborted);
    return Results.Text(body, "application/json", statusCode: (int)resp.StatusCode);
});

//app.MapGet("/api/transitiongraph/health", () => new { status = "OK" });
app.MapGet("/health", () => new { status = "OK" });

app.MapPost("/internal/shutdown", (IHostApplicationLifetime life) =>
{
    life.StopApplication();
    return Results.Ok(new { message = "shutting down" });
});

app.Lifetime.ApplicationStopping.Register(() =>
{
    try { outputMgr.ShutdownAsync().GetAwaiter().GetResult(); }
    catch { /* ignore */ }
});

app.Run();

// -------------------------------
// Helpers
// -------------------------------

/// <summary>
/// ServerEndpointStore-ServerEndpointStore
/// 処理概略：ServerEndpointの保存/管理(Store)を行う
/// </summary>
public sealed class ServerEndpointStore
{
    private readonly object _gate = new();
    private string? _baseUrl;

    public bool HasValue
    {
        get { lock (_gate) return !string.IsNullOrWhiteSpace(_baseUrl); }
    }

    /// <summary>
    /// Set-Set
    /// 処理概略：処理を設定する
    /// </summary>
    public void Set(string baseUrl)
    {
        lock (_gate) _baseUrl = baseUrl.TrimEnd('/');
    }

    /// <summary>
    /// GetOrDefault-取得OrDefault
    /// 処理概略：取得OrDefaultの決定
    /// </summary>
    public string GetOrDefault(string fallback)
    {
        lock (_gate) return string.IsNullOrWhiteSpace(_baseUrl) ? fallback.TrimEnd('/') : _baseUrl!;
    }
}

/// <summary>
/// OutputProcessManager-OutputProcessマネージャ
/// 処理概略：OutputProcessの管理(Manager)を行う
/// </summary>
public sealed class OutputProcessManager
{
    private Process? _proc;
    private readonly HttpClient _http = new(new SocketsHttpHandler { AutomaticDecompression = DecompressionMethods.All });
    private string _baseUrl = PortSettingStore.Current.OutputBaseUrl;

    /// <summary>
    /// StartAsync-Start（非同期）
    /// 処理概略：Asyncを開始する
    /// </summary>
    public async Task StartAsync(IConfiguration cfg)
    {
        _baseUrl = cfg.GetValue<string>("Output:BaseUrl") ?? _baseUrl;

        var enable = cfg.GetValue<bool?>("Output:Enabled") ?? true;
        if (!enable) return;

        var exe = cfg.GetValue<string>("Output:Exe") ?? "CTransitionGraphOutput.exe";
        var port = cfg.GetValue<int?>("Output:Port") ?? PortSettingStore.Current.OutputPort;
        var waitSeconds = cfg.GetValue<int?>("Output:StartupWaitSeconds") ?? 10;

        // Resolve exe
        string? fullExe = ResolveExe(exe);
        if (fullExe is null)
        {
            MessageOut.MessageOutPut($"CTransitionGraphAPI: Output exe not found, skip spawn. configured={exe}");
            return;
        }

        var psi = new ProcessStartInfo
        {
            FileName = fullExe,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = System.Text.Encoding.UTF8,
            StandardErrorEncoding = System.Text.Encoding.UTF8,
        };
        psi.ArgumentList.Add("--port");
        psi.ArgumentList.Add(port.ToString());

        var p = new Process { StartInfo = psi, EnableRaisingEvents = true };
        p.OutputDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data)) Console.WriteLine($"[Output STDOUT] {e.Data}"); };
        p.ErrorDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data)) Console.WriteLine($"[Output STDERR] {e.Data}"); };

        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        _proc = p;

        // wait for health
        _http.Timeout = TimeSpan.FromSeconds(2);
        var deadline = DateTime.UtcNow.AddSeconds(waitSeconds);
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var resp = await _http.GetAsync(_baseUrl.TrimEnd('/') + "/health");
                if (resp.IsSuccessStatusCode) break;
            }
            catch { /* retry */ }
            await Task.Delay(300);
        }

        _http.Timeout = TimeSpan.FromSeconds(10);
    }

    /// <summary>
    /// SendAsync-Send（非同期）
    /// 処理概略：SendAsyncを処理する
    /// </summary>
    public async Task SendAsync(RequestHeaderDto header, CancellationToken ct)
    {
        try
        {
            var url = _baseUrl.TrimEnd('/') + "/api/transitiongraph/output";
            var json = JsonSerializer.Serialize(header);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            await _http.PostAsync(url, content, ct);
        }
        catch (Exception ex)
        {
            MessageOut.MessageOutPut($"CTransitionGraphAPI: Output send failed: {ex.Message}");
        }
    }

    /// <summary>
    /// ShutdownAsync-Shutdown（非同期）
    /// 処理概略：ShutdownAsyncを処理する
    /// </summary>
    public async Task ShutdownAsync()
    {
        try
        {
            _http.Timeout = TimeSpan.FromSeconds(3);
            await _http.PostAsync(_baseUrl.TrimEnd('/') + "/internal/shutdown", content: null);
        }
        catch { /* ignore */ }

        try
        {
            if (_proc is { HasExited: false })
            {
                if (!_proc.WaitForExit(3000))
                    _proc.Kill(entireProcessTree: true);
            }
        }
        catch { /* ignore */ }
    }

    /// <summary>
    /// ResolveExe-ResolveExe
    /// 処理概略：Exeを解決する
    /// </summary>
    private static string? ResolveExe(string exePathOrName)
    {
        if (string.IsNullOrWhiteSpace(exePathOrName)) return null;
        if (Path.IsPathRooted(exePathOrName)) return File.Exists(exePathOrName) ? exePathOrName : null;

        var baseDir = AppContext.BaseDirectory;
        var c1 = Path.GetFullPath(Path.Combine(baseDir, exePathOrName));
        if (File.Exists(c1)) return c1;

        // common publish layout
        var c2 = Path.Combine(baseDir, "output", exePathOrName);
        if (File.Exists(c2)) return c2;

        // dev layout (repo)
        var repo = FindRepoRoot(baseDir);
        if (repo is null) return null;

        var debugDir = Path.Combine(repo, "CTransitionGraphOutput", "bin", "Debug", "net8.0");
        var releaseDir = Path.Combine(repo, "CTransitionGraphOutput", "bin", "Release", "net8.0");
        var cfgDir = Directory.Exists(debugDir) ? debugDir : (Directory.Exists(releaseDir) ? releaseDir : null);
        if (cfgDir is null) return null;

        var exeName = exePathOrName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ? exePathOrName : "CTransitionGraphOutput.exe";
        var c3 = Path.Combine(cfgDir, exeName);
        return File.Exists(c3) ? c3 : null;
    }

    /// <summary>
    /// FindRepoRoot-検索RepoRoot
    /// 処理概略：RepoRootを検索する
    /// </summary>
    private static string? FindRepoRoot(string baseDir)
    {
        var dir = new DirectoryInfo(baseDir);
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var slns = dir.GetFiles("*.sln", SearchOption.TopDirectoryOnly);
            if (slns.Length > 0) return dir.FullName;
            dir = dir.Parent;
        }
        return null;
    }
}