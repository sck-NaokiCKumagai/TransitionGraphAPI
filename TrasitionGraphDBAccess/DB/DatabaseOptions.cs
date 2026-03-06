namespace TransitionGraph.Db;

public sealed class DatabaseOptions
{
    public string Provider { get; set; } = "SqlServer"; // "SqlServer" or "Vertica_Test" or "Vertica"
    public int CommandTimeoutSeconds { get; set; } = 60;

    public SqlServerOptions SqlServer { get; set; } = new();
    public VerticaOptions VerticaTest { get; set; } = new();
    public VerticaOptions Vertica { get; set; } = new();
}

public sealed class SqlServerOptions
{
    public string ConnectionString { get; set; } = "";
}

public sealed class VerticaOptions
{
    public string ConnectionString { get; set; } = "";
}
