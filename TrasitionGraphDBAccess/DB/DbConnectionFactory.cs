using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Vertica.Data.VerticaClient;
using System.Globalization;

namespace TransitionGraph.Db;

public sealed class DbConnectionFactory : IDbConnectionFactory
{
    private readonly DatabaseOptions _opt;

    public DbProviderType ProviderType { get; }

    public DbConnectionFactory(IOptions<DatabaseOptions> options)
    {
        _opt = options.Value;

        var p = _opt.Provider?.Trim() ?? "SqlServer";

        ProviderType = p.Equals("Vertica_Test", StringComparison.OrdinalIgnoreCase)
            ? DbProviderType.VerticaTest
            : p.Equals("Vertica", StringComparison.OrdinalIgnoreCase)
                ? DbProviderType.Vertica
                : DbProviderType.SqlServer;
    }

    public async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        string cs = ProviderType switch
        {
            DbProviderType.SqlServer => _opt.SqlServer.ConnectionString,
            DbProviderType.VerticaTest => _opt.VerticaTest.ConnectionString,
            DbProviderType.Vertica => _opt.Vertica.ConnectionString,
            _ => throw new InvalidOperationException("Unknown DB provider.")
        };

        if (string.IsNullOrWhiteSpace(cs))
            throw new InvalidOperationException($"ConnectionString is empty. Provider={ProviderType}. Check DBAccess appsettings.json or env override.");

        DbConnection conn = ProviderType switch
        {
            DbProviderType.SqlServer => new SqlConnection(cs),
            DbProviderType.Vertica => new VerticaConnection(cs),
            _ => throw new InvalidOperationException("Unknown DB provider.")
        };

        await conn.OpenAsync(ct);
        return conn;
    }

}
