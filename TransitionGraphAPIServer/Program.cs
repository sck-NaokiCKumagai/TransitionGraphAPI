using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

internal sealed class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        // Windowsサービスとして動かす（コンソール実行でも動作します）
        builder.Services.AddWindowsService(options =>
        {
            options.ServiceName = "TransitionGraphAPIServer";
        });

        builder.Services.AddHostedService<WrapperHostedService>();

        builder.Build().Run();
    }
}
