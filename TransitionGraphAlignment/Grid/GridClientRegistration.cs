using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;

namespace TransitionGraphAlignment.Grid;

public static class GridClientRegistration
{
    /// <summary>
    /// Backward-compatible overload.
    /// Uses AppContext.BaseDirectory as dataDir.
    /// </summary>
    public static IServiceCollection AddGridClient(this IServiceCollection services, IConfiguration config)
        => services.AddGridClient(config, AppContext.BaseDirectory);

    /// <summary>
    /// Register IGridClient. Switched by configuration:
    ///
    /// GridDll:
    ///   UseDummy: true/false
    ///   DummyCsvDir: (optional) csv directory
    ///
    /// If DummyCsvDir is not set, defaults to {dataDir}/GridCsv.
    /// </summary>
    public static IServiceCollection AddGridClient(this IServiceCollection services, IConfiguration config, string dataDir)
    {
        File.AppendAllText(@"C:\temp\tg_addgridclient.txt",    $"HIT AddGridClient baseDir={AppContext.BaseDirectory} cwd={Directory.GetCurrentDirectory()} raw={config["GridDll:UseDummy"]}\r\n");

        // If appsettings.json is not found (e.g., started from a different working directory),
        // GetValue<bool> returns false by default. In this project, the real DXC dll is not usable
        // in the dev/test environment, so default to Dummy when the key is missing.
        bool useDummy;
        var raw = config["GridDll:UseDummy"];
        if (string.IsNullOrWhiteSpace(raw))
        {
            useDummy = true;
        }
        else
        {
            useDummy = config.GetValue<bool>("GridDll:UseDummy");
        }

        if (useDummy)
        {
            var csvDir = config.GetValue<string>("GridDll:DummyCsvDir");
            if (string.IsNullOrWhiteSpace(csvDir))
                csvDir = Path.Combine(dataDir, "GridCsv");

            // Allow relative path.
            if (!Path.IsPathRooted(csvDir))
                csvDir = Path.Combine(dataDir, csvDir);

            services.AddSingleton<IGridClient>(new DummyGridClient(csvDir));
        }
        else
        {
            services.AddSingleton<IGridClient, RealGridClient>();
        }

        return services;
    }
}
