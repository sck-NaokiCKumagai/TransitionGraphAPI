using System.Text.Json;

namespace TransitionGraph.Db;

/// <summary>
/// SQL 定型文カタログ。
/// 画面→Host→DBAccess の要求は「sqlKey + parameters」で流れてくる前提。
/// DBAccess は sqlKey から SQL を引き当て、parameters を WHERE に反映して実行する。
/// </summary>
public interface ISqlCatalog
{
    /// <summary>ページング取得用SQLテンプレート（{{where}} を含める想定）</summary>
    string GetFetchPageSql(string sqlKey);

    /// <summary>sqlKey ごとに許可するパラメータ名一覧</summary>
    IReadOnlyCollection<string> GetAllowedParameters(string sqlKey);
}

/// <summary>
/// Json 形式の SQL カタログ。
/// provider は "SqlServer" / "Vertica"
/// </summary>
public sealed class JsonSqlCatalog : ISqlCatalog
{
    private readonly string _path;
    private readonly string _provider;
    private readonly CatalogRoot _root;

    public JsonSqlCatalog(string path, string provider)
    {
        _path = path;
        _provider = provider;
        _root = Load(path);
    }

    public string GetFetchPageSql(string sqlKey)
    {
        if (!_root.Queries.TryGetValue(sqlKey, out var q))
            throw new KeyNotFoundException($"SqlCatalog: sqlKey not found: {sqlKey}");

        var p = _provider.Equals("Vertica_Test", StringComparison.OrdinalIgnoreCase)
            ? (q.VerticaTest ?? q.Vertica)
            : _provider.Equals("Vertica", StringComparison.OrdinalIgnoreCase)
                ? q.Vertica
                : q.SqlServer;
        if (string.IsNullOrWhiteSpace(p))
            throw new InvalidOperationException($"SqlCatalog: SQL text empty. sqlKey={sqlKey} provider={_provider}");
        return p;
    }

    public IReadOnlyCollection<string> GetAllowedParameters(string sqlKey)
    {
        if (!_root.Queries.TryGetValue(sqlKey, out var q))
            return Array.Empty<string>();
        return q.AllowedParameters ?? Array.Empty<string>();
    }

    private static CatalogRoot Load(string path)
    {
        if (!File.Exists(path))
        {
            // ない場合は空で作る（プロジェクト雛形で落ちないため）
            return new CatalogRoot();
        }

        var json = File.ReadAllText(path);
        var root = JsonSerializer.Deserialize<CatalogRoot>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        return root ?? new CatalogRoot();
    }

    private sealed class CatalogRoot
    {
        public Dictionary<string, CatalogQuery> Queries { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class CatalogQuery
    {
        public string? SqlServer { get; set; }
        public string? Vertica { get; set; }
        public string? VerticaTest { get; set; }
        public string[]? AllowedParameters { get; set; }
    }
}
