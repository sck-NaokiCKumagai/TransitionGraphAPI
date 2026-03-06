using System.Collections.Concurrent;

namespace TransitionGraph.Db;

/// <summary>
/// OpenQuery で受け取った (sqlKey, parameters) を FetchPage 時に参照するための保持領域。
/// 初期段階はプロセス内メモリ保持（要件が固まったら永続化/分散化も可）。
/// </summary>
public sealed record QueryContext(Guid ResultId, string SqlKey, IReadOnlyDictionary<string, string> Parameters);

public interface IQueryRegistry
{
    void Register(QueryContext ctx);
    bool TryGet(Guid resultId, out QueryContext ctx);

    /// <summary>
    /// Remove a registered query context (used for cancel/cleanup).
    /// </summary>
    bool Remove(Guid resultId);
}

public sealed class InMemoryQueryRegistry : IQueryRegistry
{
    private readonly ConcurrentDictionary<Guid, QueryContext> _map = new();

    public void Register(QueryContext ctx) => _map[ctx.ResultId] = ctx;

    public bool TryGet(Guid resultId, out QueryContext ctx)
        => _map.TryGetValue(resultId, out ctx!);

    public bool Remove(Guid resultId)
        => _map.TryRemove(resultId, out _);
}
