using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// Central routing logic for choosing the backend based on RequestId.
/// Spec (5):
///  - 1-6 -> DBAccess (1 may be local)
///  - 7-9 -> Alignment (7 also builds ISTARInfo locally after sync)
/// </summary>
public sealed class BackendRouter
{
    private readonly LocalBackendService _local;
    private readonly DbAccessBackendService _db;
    private readonly AlignmentBackendService _alignment;
    private readonly IstarBackendService _istar;
    private readonly GridFilterSortBackendService _gridFilterSort;

    public BackendRouter(LocalBackendService local, DbAccessBackendService db, AlignmentBackendService alignment, IstarBackendService istar, GridFilterSortBackendService gridFilterSort)
    {
        _local = local;
        _db = db;
        _alignment = alignment;
        _istar = istar;
        _gridFilterSort = gridFilterSort;
    }

    public (IBackendService backend, BackendKind kind) Select(string requestId)
    {
        // RequestId 10 -> Grid DLL (filter/sort) + fixed response
        if (requestId is "10")
            return (_gridFilterSort, BackendKind.Local);

        // RequestId 7 -> Alignment sync + local ISTARInfo build
        if (requestId is "7")
            return (_istar, BackendKind.Alignment);

        // RequestId 8-9 -> Alignment
        if (requestId is "8" or "9")
            return (_alignment, BackendKind.Alignment);

        // RequestId 1 -> local fixed response
        if (requestId is "1")
            return (_local, BackendKind.Local);

        // default -> DBAccess
        return (_db, BackendKind.DbAccess);
    }
}
