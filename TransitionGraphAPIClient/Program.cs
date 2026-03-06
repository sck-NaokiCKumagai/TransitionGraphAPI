using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Windows Service起動にも対応（コンソール起動でも動きます）
builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "TransitionGraphAPIClient";
});

// 子プロセス起動・監視（CTransitionGraphAPI / CTransitionGraphOutput）
builder.Services.AddHostedService<WrapperHostedService>();

var host = builder.Build();
await host.RunAsync();
