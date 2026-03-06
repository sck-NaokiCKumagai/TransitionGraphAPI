using System.Data.Common;

namespace TransitionGraph.Db;

public interface ISqlDialect
{
    // SQL中で使うプレースホルダ表記（@p / :p）
    string Param(string name);

    // DbParameter の ParameterName に設定する名前（@p or p）
    string ParameterName(string name);

    // ページ取得SQLを生成（TOP/LIMIT差分吸収）
    string BuildFetchPageSql(string? whereClause = null);

    // 共通のパラメータ追加
    DbParameter AddParameter(DbCommand cmd, string name, object? value);
}
