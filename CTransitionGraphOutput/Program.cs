using Microsoft.AspNetCore.Server.Kestrel.Core;
using TransitionGraph.Dtos;
using TransitionGraph.LogOutPut;
using TransitionGraph.PortSetting;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

var port = builder.Configuration.GetValue<int?>("App:ListenPort") ?? PortSettingStore.Current.OutputPort;
for (int i = 0; i + 1 < args.Length; i++)
{
    if (args[i].Equals("--port", StringComparison.OrdinalIgnoreCase) && int.TryParse(args[i + 1], out var p))
        port = p;
}

builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenLocalhost(port, lo => lo.Protocols = HttpProtocols.Http1AndHttp2);
});

builder.Services.AddResponseCompression();

var app = builder.Build();
app.UseResponseCompression();

// Health endpoints (for wrapper/parent)
app.MapGet("/health", () => new { status = "OK" });
app.MapGet("/api/transitiongraph/health", () => new { status = "OK" });

// Internal shutdown
app.MapPost("/internal/shutdown", (IHostApplicationLifetime life) =>
{
    life.StopApplication();
    return Results.Ok(new { message = "shutting down" });
});

// Receive output request (RequestId=11 expected)
app.MapPost("/api/transitiongraph/output", (RequestHeaderDto header) =>
{
    // 受信し以降の処理につなげられるところまで
    MessageOut.MessageOutPut($"CTransitionGraphOutput: received RequestId={header.RequestId} GUID={header.GUID} Seq={header.SequenceNo}");

    // TODO: ここから先の処理詳細は保留
    // 例: キューに積む / ファイルに保存 / 次工程へ通知 etc.

    return Results.Accepted();
});

app.Run();
