using System.Collections.Concurrent;
using TransitionGraph.V1;

namespace TransitionGraphAPI;

/// <summary>
/// DBから取り出した全件データをメモリ保持し、pageSize 件ごとにページングして返すためのストア。
///
/// - /api/transitiongraph/query で全件取得し、resultId をキーに保存
/// - /api/transitiongraph/result/{resultId} で cursor(lastRowNo) を使って分割返却
/// </summary>
public sealed class ResultSetMemoryStore
{
    /// <summary>
    /// Entry-エントリー処理
    /// 処理概略：エントリー処理をまとめる
    /// </summary>
    private sealed record Entry(IReadOnlyList<Point> Items);

    private readonly ConcurrentDictionary<string, Entry> _store = new(StringComparer.OrdinalIgnoreCase);

    public void Put(string resultId, IReadOnlyList<Point> items)
        => _store[resultId] = new Entry(items);

    /// <summary>
    /// TryGet-試行取得
    /// 処理概略：Getを試行する
    /// </summary>
    public bool TryGet(string resultId, out IReadOnlyList<Point> items)
    {
        if (_store.TryGetValue(resultId, out var e))
        {
            items = e.Items;
            return true;
        }
        items = Array.Empty<Point>();
        return false;
    }

    /// <summary>
    /// Remove-削除処理
    /// 処理概略：削除処理
    /// </summary>
    public void Remove(string resultId)
        => _store.TryRemove(resultId, out _);
}