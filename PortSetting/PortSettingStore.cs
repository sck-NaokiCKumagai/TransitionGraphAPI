namespace TransitionGraph.PortSetting;

/// <summary>
/// Loads PortSetting.properties once per process and exposes the resolved values.
/// </summary>
public static class PortSettingStore
{
    private static readonly Lazy<PortSettingValues> _current = new(LoadInternal);

    /// <summary>
    /// Current resolved port settings.
    /// </summary>
    public static PortSettingValues Current => _current.Value;

    /// <summary>
    /// Default file name.
    /// </summary>
    public const string DefaultFileName = "PortSetting.properties";

    /// <summary>
    /// Environment variable that can point to PortSetting.properties (absolute or relative).
    /// </summary>
    public const string EnvVarPath = "TRANSITIONGRAPH_PORTSETTING_PATH";

    /// <summary>
    /// LoadInternal-ì«çûInternal
    /// èàóùäTó™ÅFInternalÇì«Ç›çûÇ›Ç∑ÇÈ
    /// </summary>
    private static PortSettingValues LoadInternal()
    {
        // 1) Env-var override
        var env = Environment.GetEnvironmentVariable(EnvVarPath);
        if (!string.IsNullOrWhiteSpace(env))
        {
            var p = Path.IsPathRooted(env)
                ? env
                : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, env));
            return LoadFrom(p);
        }

        // 2) Next to exe
        var filePath = Path.Combine(AppContext.BaseDirectory, DefaultFileName);
        return LoadFrom(filePath);
    }

    /// <summary>
    /// LoadFrom-ì«çûÅ©
    /// èàóùäTó™ÅFFromÇì«Ç›çûÇ›Ç∑ÇÈ
    /// </summary>
    private static PortSettingValues LoadFrom(string filePath)
    {
        var props = PropertiesFile.Load(filePath);

        // NOTE: Keep defaults consistent with existing behavior.
        var scheme = PropertiesFile.GetString(props, "Common.Scheme", PropertiesFile.GetString(props, "Scheme", "http"));

        var host = PropertiesFile.GetString(props, "Common.Host", PropertiesFile.GetString(props, "Host", "127.0.0.1"));

        return new PortSettingValues
        {
            Scheme = scheme,
            Host = host,
            ApiServerPort = PropertiesFile.GetInt(props, "ApiServer.Port", 5000),
            ApiClientPort = PropertiesFile.GetInt(props, "ApiClient.Port", 5500),
            OutputPort = PropertiesFile.GetInt(props, "Output.Port", 5203),

            WorkerDataEditPort = PropertiesFile.GetInt(props, "Workers.DataEdit.Port", 5101),
            WorkerDbAccessPort = PropertiesFile.GetInt(props, "Workers.DbAccess.Port", 5102),
            WorkerOutputPort = PropertiesFile.GetInt(props, "Workers.Output.Port", 5103),
            WorkerAlignmentPort = PropertiesFile.GetInt(props, "Workers.Alignment.Port", 5104),
        };
    }
}