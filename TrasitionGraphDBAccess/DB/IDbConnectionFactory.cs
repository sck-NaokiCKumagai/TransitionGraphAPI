using System.Data.Common;

namespace TransitionGraph.Db;

public interface IDbConnectionFactory
{
    DbProviderType ProviderType { get; }
    Task<DbConnection> OpenConnectionAsync(CancellationToken ct);
}
