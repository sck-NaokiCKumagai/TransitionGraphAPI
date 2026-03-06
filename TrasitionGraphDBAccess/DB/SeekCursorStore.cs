using System.Collections.Concurrent;

namespace TransitionGraph.Db;

/// <summary>
/// Keyset (seek) paging cursor store.
/// We reuse the existing gRPC "lastRowNo" integer as an opaque cursor id.
/// cursorId=0 means "from beginning".
/// </summary>
public interface ISeekCursorStore
{
    bool TryGet(Guid resultId, int cursorId, out SeekCursor cursor);
    int Save(Guid resultId, SeekCursor cursor);
    void Clear(Guid resultId);
}

public sealed record SeekCursor(string LastLotName, string LastTestName, int LastWaferNo, int LastChipNo);

public sealed class InMemorySeekCursorStore : ISeekCursorStore
{
    private sealed class Bucket
    {
        public int NextId = 1;
        public ConcurrentDictionary<int, SeekCursor> Map { get; } = new();
    }

    private readonly ConcurrentDictionary<Guid, Bucket> _buckets = new();

    public bool TryGet(Guid resultId, int cursorId, out SeekCursor cursor)
    {
        cursor = default!;
        if (cursorId <= 0) return false;
        return _buckets.TryGetValue(resultId, out var b) && b.Map.TryGetValue(cursorId, out cursor);
    }

    public int Save(Guid resultId, SeekCursor cursor)
    {
        var b = _buckets.GetOrAdd(resultId, _ => new Bucket());
        var id = Interlocked.Increment(ref b.NextId);
        b.Map[id] = cursor;
        return id;
    }

    public void Clear(Guid resultId)
    {
        _buckets.TryRemove(resultId, out _);
    }
}
