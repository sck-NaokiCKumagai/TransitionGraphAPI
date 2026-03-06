using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;

namespace TransitionGraphDataEdit.Grid;

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
            // RealGridClient needs IConfiguration to build the production URL.
            services.AddSingleton<IGridClient>(sp => new RealGridClient(config, sp.GetService<Microsoft.Extensions.Logging.ILogger<RealGridClient>>()));
        }

        return services;
    }
}
