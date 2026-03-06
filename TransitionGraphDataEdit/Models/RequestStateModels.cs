using System.Collections.Concurrent;
using TransitionGraph.V1;

namespace TransitionGraphDataEdit.Models;

/// <summary>
/// Which backend actually executed the request.
/// Spec (5):
///  - 1-6 -> DbAccess (1 may be Local)
///  - 7   -> DataEdit (Grid)
///  - 8-9 -> Alignment
/// </summary>
public enum BackendKind
{
    Local = 0,
    DbAccess = 1,
    Alignment = 2,
}

/// <summary>
/// In-memory state for one request instance, used for cancel + cleanup.
/// </summary>
public sealed class RequestState
{
    public RequestState(
        string guid,
        string requestId,
        string requestInstanceId,
        string resultId,
        CancellationTokenSource? cts,
        string? signature,
        BackendKind backend,
        ConcurrentBag<string> cacheKeys)
    {
        Guid = guid;
        RequestId = requestId;
        RequestInstanceId = requestInstanceId;
        ResultId = resultId;
        Cts = cts;
        Signature = signature;
        Backend = backend;
        CacheKeys = cacheKeys;
    }

    public string Guid { get; }
    public string RequestId { get; }
    public string RequestInstanceId { get; }

    /// <summary>ResultStore id (csv file name).</summary>
    public string ResultId { get; set; }

    /// <summary>Cancellation token source for this request instance.</summary>
    public CancellationTokenSource? Cts { get; }

    /// <summary>
    /// Only used by RequestId=2 auto-cancel logic.
    /// signature = process|startTime|endTime.
    /// </summary>
    public string? Signature { get; }

    public BackendKind Backend { get; }

    /// <summary>
    /// GlobalCache keys created by this request (metadata cache).
    /// Cleared on Cancel (3-3).
    /// </summary>
    public ConcurrentBag<string> CacheKeys { get; }
}

/// <summary>
/// Stores active request states for cancellation and cleanup.
/// </summary>
public sealed class RequestStateStore
{
    private readonly ConcurrentDictionary<string, RequestState> _byInstanceId = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<(string Guid, string RequestId), string> _latestByKey = new();
    private readonly ConcurrentDictionary<string, (string Signature, string InstanceId)> _key2ByGuid = new(StringComparer.OrdinalIgnoreCase);

    public void Register(RequestHeader header, string resultId, CancellationTokenSource cts, string? signature, BackendKind backend)
    {
        var instanceId = header.RequestInstanceId ?? string.Empty;
        if (string.IsNullOrWhiteSpace(instanceId))
            throw new ArgumentException("header.requestInstanceId is required", nameof(header));

        var state = new RequestState(
            guid: header.Guid ?? string.Empty,
            requestId: header.RequestId ?? string.Empty,
            requestInstanceId: instanceId,
            resultId: resultId,
            cts: cts,
            signature: signature,
            backend: backend,
            cacheKeys: new ConcurrentBag<string>());

        _byInstanceId[instanceId] = state;
        _latestByKey[(state.Guid, state.RequestId)] = instanceId;

        if (string.Equals(state.RequestId, "2", StringComparison.OrdinalIgnoreCase) && signature is not null)
            _key2ByGuid[state.Guid] = (signature, instanceId);
    }

    public bool TryGet(string requestInstanceId, out RequestState state)
        => _byInstanceId.TryGetValue(requestInstanceId, out state!);

    public void UpdateResultId(string requestInstanceId, string newResultId)
    {
        if (_byInstanceId.TryGetValue(requestInstanceId, out var st))
            st.ResultId = newResultId;
    }

    public void AddCacheKey(string requestInstanceId, string cacheKey)
    {
        if (_byInstanceId.TryGetValue(requestInstanceId, out var st))
            st.CacheKeys.Add(cacheKey);
    }

    /// <summary>
    /// Spec (3-2): for RequestId=2, if (process/start/end) changed for the same guid,
    /// treat the previous request as canceled.
    /// </summary>
    public bool TryCancelPreviousForKey2Change(string guid, string newSignature, out RequestState previous)
    {
        previous = default!;

        if (!_key2ByGuid.TryGetValue(guid, out var prevInfo))
            return false;

        if (string.Equals(prevInfo.Signature, newSignature, StringComparison.Ordinal))
            return false;

        if (_byInstanceId.TryGetValue(prevInfo.InstanceId, out var st))
        {
            try { st.Cts?.Cancel(); } catch { /* ignore */ }
            previous = st;
            return true;
        }
        return false;
    }

    /// <summary>
    /// Cancel by requestInstanceId if available; otherwise by (guid, requestId).
    /// </summary>
    public bool TryCancel(RequestHeader header, string _unusedResultId, out RequestState state)
    {
        state = default!;

        var instanceId = header.RequestInstanceId;
        if (!string.IsNullOrWhiteSpace(instanceId) && _byInstanceId.TryGetValue(instanceId, out var st1))
        {
            try { st1.Cts?.Cancel(); } catch { /* ignore */ }
            state = st1;
            return true;
        }

        var guid = header.Guid ?? string.Empty;
        var reqId = header.RequestId ?? string.Empty;

        if (_latestByKey.TryGetValue((guid, reqId), out var latestInstanceId)
            && _byInstanceId.TryGetValue(latestInstanceId, out var st2))
        {
            try { st2.Cts?.Cancel(); } catch { /* ignore */ }
            state = st2;
            return true;
        }

        return false;
    }

    public void Complete(string requestInstanceId)
    {
        if (string.IsNullOrWhiteSpace(requestInstanceId))
            return;

        if (_byInstanceId.TryRemove(requestInstanceId, out var st))
        {
            _latestByKey.TryRemove((st.Guid, st.RequestId), out _);

            if (string.Equals(st.RequestId, "2", StringComparison.OrdinalIgnoreCase))
            {
                // remove only if it points to this instance
                if (_key2ByGuid.TryGetValue(st.Guid, out var info) &&
                    string.Equals(info.InstanceId, requestInstanceId, StringComparison.OrdinalIgnoreCase))
                {
                    _key2ByGuid.TryRemove(st.Guid, out _);
                }
            }
        }
    }
}
