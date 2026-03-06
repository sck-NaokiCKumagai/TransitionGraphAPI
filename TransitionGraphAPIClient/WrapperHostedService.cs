using System.Diagnostics;
using System.Net.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using TransitionGraph.LogOutPut;
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
            args = $"--urls http://{ports.Host}:{ports.ApiClientPort}";

        var apiBase = section.GetValue<string>("ApiBaseUrl") ?? ports.ApiClientBaseUrl;
        var healthPath = section.GetValue<string>("HealthPath") ?? "/health";
        var startupWaitSeconds = section.GetValue<int?>("StartupWaitSeconds") ?? 30;

        var exePath = Path.IsPathRooted(exeRel) ? exeRel : Path.Combine(AppContext.BaseDirectory, exeRel);

        exePath = Path.GetFullPath(exePath);
        if (string.IsNullOrWhiteSpace(workDir))
            workDir = Path.GetDirectoryName(exePath) ?? AppContext.BaseDirectory;

        MessageOut.MessageOutPut("Wrapper starting target exe: {ExePath}", exePath);

        _job = new JobObject();

        var psi = new ProcessStartInfo
        {
            FileName = exePath,
            Arguments = args,
            WorkingDirectory = workDir,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        MessageOut.MessageOutPut("Starting target: {Exe} {Args} ", psi.FileName, psi.Arguments);

        if (!string.IsNullOrWhiteSpace(args))
            psi.Arguments = args;

        MessageOut.MessageOutPut("Starting child: {ChildPath}, wd={WD}", exePath, psi.WorkingDirectory);

        var p = new Process { StartInfo = psi, EnableRaisingEvents = true };
        p.Start();
        _job.AddProcess(p.Handle);
        _proc = p;

        // 起動待ち（healthが返るまで待つ）
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var deadline = DateTime.UtcNow.AddSeconds(startupWaitSeconds);
            var url = apiBase.TrimEnd('/') + healthPath;

            while (DateTime.UtcNow < deadline && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var resp = await http.GetAsync(url, stoppingToken);
                    if (resp.IsSuccessStatusCode)
                    {
                        MessageOut.MessageOutPut("Target is healthy: {Url}", url);
                        break;
                    }
                    _logger.LogWarning("Health check non-success: {Url} status={Status}", url, (int)resp.StatusCode);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Health check failed: {Url}", url);
                }

                await Task.Delay(500, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Startup wait failed (continuing).");
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
        var apiBase = section.GetValue<string>("ApiBaseUrl") ?? PortSettingStore.Current.ApiClientBaseUrl;
        var shutdownPath = section.GetValue<string>("ShutdownPath") ?? "/internal/shutdown";
        var shutdownWaitSeconds = section.GetValue<int?>("ShutdownWaitSeconds") ?? 20;

        try
        {
            // まずはAPIでgraceful shutdownを試みる
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var url = apiBase.TrimEnd('/') + shutdownPath;
            MessageOut.MessageOutPut("Sending graceful shutdown: {Url}", url);
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
