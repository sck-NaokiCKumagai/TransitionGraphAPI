using System.Diagnostics;
using System.Net.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using TransitionGraph.PortSetting;

internal sealed class WrapperHostedService : BackgroundService
{
    private readonly ILogger<WrapperHostedService> _logger;
    private readonly IConfiguration _cfg;

    private Process? _proc;
    private JobObject? _job;
    private volatile bool _stopping;

    public WrapperHostedService(ILogger<WrapperHostedService> logger, IConfiguration cfg)
    {
        _logger = logger;
        _cfg = cfg;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var ports = PortSettingStore.Current;
        var section = _cfg.GetSection("Wrapper");
        var exeRel = section.GetValue<string>("TargetExeRelativePath") ?? "";
        var workDir = section.GetValue<string>("TargetWorkingDir") ?? "";
        var args = section.GetValue<string>("TargetArgs") ?? "";
        if (string.IsNullOrWhiteSpace(args))
            args = $"--urls http://{ports.Host}:{ports.ApiServerPort}";

        var apiBase = section.GetValue<string>("ApiBaseUrl") ?? ports.ApiServerBaseUrl;
        var healthPath = section.GetValue<string>("HealthPath") ?? "/health";
        var startupWaitSeconds = section.GetValue<int?>("StartupWaitSeconds") ?? 30;

        var exePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, exeRel));
        if (string.IsNullOrWhiteSpace(workDir))
            workDir = Path.GetDirectoryName(exePath) ?? AppContext.BaseDirectory;

        _logger.LogInformation("Wrapper starting target exe: {ExePath}", exePath);

        if (!File.Exists(exePath))
        {
            _logger.LogError("Wrapper: target exe not found: {ExePath}", exePath);
            Environment.ExitCode = 1;
            Environment.Exit(1);
            return;
        }

        _job = new JobObject();

        var psi = new ProcessStartInfo
        {
            FileName = exePath,
            WorkingDirectory = workDir,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = System.Text.Encoding.UTF8,
            StandardErrorEncoding = System.Text.Encoding.UTF8,
        };
        if (!string.IsNullOrWhiteSpace(args))
            psi.Arguments = args;

        /*****
        var p = new Process { StartInfo = psi, EnableRaisingEvents = true };
        p.Start();
        _job.AddProcess(p.Handle);
        _proc = p;

        // 起動待ち（healthが返るまで待つ）
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
            var deadline = DateTime.UtcNow.AddSeconds(startupWaitSeconds);
            while (DateTime.UtcNow < deadline && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var resp = await http.GetAsync(apiBase.TrimEnd('/') + healthPath, stoppingToken);
                    if (resp.IsSuccessStatusCode)
                    {
                        _logger.LogInformation("Target is healthy: {Url}", apiBase.TrimEnd('/') + healthPath);
                        break;
                    }
                }
                catch {  }

                await Task.Delay(500, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Startup wait failed (continuing).");
        }
        *****/

        Process p;
        try
        {
            p = new Process { StartInfo = psi, EnableRaisingEvents = true };
            _logger.LogInformation("Starting child: {ChildPath}, wd={WD} args={Args}", exePath, workDir, psi.Arguments);

            p.Start();

            // ★出力を拾ってログに流す
            _ = Task.Run(async () =>
            {
                while (!p.HasExited)
                {
                    var line = await p.StandardOutput.ReadLineAsync();
                    if (line is null) break;
                    _logger.LogInformation("[Target STDOUT] {Line}", line);
                }
            });

            _ = Task.Run(async () =>
            {
                while (!p.HasExited)
                {
                    var line = await p.StandardError.ReadLineAsync();
                    if (line is null) break;
                    _logger.LogError("[Target STDERR] {Line}", line);
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Wrapper: failed to start target exe. exe={ExePath} wd={WD} args={Args}", exePath, workDir, args);
            Environment.ExitCode = 1;
            Environment.Exit(1);
            return;
        }

        // ここからは「落ちたらサービスも落ちる」：回復設定で再起動させる想定
        try
        {
            await p.WaitForExitAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // stopping
        }

        if (_stopping)
            return;

        var code = p.HasExited ? p.ExitCode : -1;
        _logger.LogError("Target process exited unexpectedly. ExitCode={ExitCode}", code);

        // SCM に「失敗」として認識させるため非0で終了
        Environment.ExitCode = 1;
        Environment.Exit(1);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping = true;

        var section = _cfg.GetSection("Wrapper");
        var apiBase = section.GetValue<string>("ApiBaseUrl") ?? PortSettingStore.Current.ApiServerBaseUrl;
        var shutdownPath = section.GetValue<string>("ShutdownPath") ?? "/internal/shutdown";
        var shutdownWaitSeconds = section.GetValue<int?>("ShutdownWaitSeconds") ?? 20;

        try
        {
            // まずはAPIでgraceful shutdownを試みる
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var url = apiBase.TrimEnd('/') + shutdownPath;
            _logger.LogInformation("Sending graceful shutdown: {Url}", url);
            await http.PostAsync(url, content: null, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Graceful shutdown request failed.");
        }

        // 子プロセスが自然に落ちるのを待つ
        try
        {
            if (_proc != null && !_proc.HasExited)
            {
                var deadline = DateTime.UtcNow.AddSeconds(shutdownWaitSeconds);
                while (DateTime.UtcNow < deadline && !_proc.HasExited && !cancellationToken.IsCancellationRequested)
                    await Task.Delay(200, cancellationToken);
            }
        }
        catch { /* ignore */ }

        // まだ生きていたら kill（JobObject でも落ちるが、明示的に）
        try
        {
            if (_proc != null && !_proc.HasExited)
            {
                _logger.LogWarning("Killing target process...");
                _proc.Kill(entireProcessTree: true);
            }
        }
        catch { /* ignore */ }

        _job?.Dispose();
        _job = null;

        await base.StopAsync(cancellationToken);
    }
}
