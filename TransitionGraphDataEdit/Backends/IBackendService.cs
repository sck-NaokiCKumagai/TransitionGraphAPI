using TransitionGraph.V1;
using TransitionGraphDataEdit.Models;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// Downstream backend abstraction.
///
/// - RequestId 1-6 -> DBAccess (or local fixed response)
/// - RequestId 7-9 -> Alignment
///
/// This interface exists to keep routing / cancel propagation tidy.
/// </summary>
public interface IBackendService
{
    /// <summary>
    /// Executes a request and returns a <see cref="BuildReply"/>.
    /// Each backend is responsible for persisting its result into ResultStore.
    /// </summary>
    Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken);

    /// <summary>
    /// Best-effort cancel on the backend side.
    /// </summary>
    Task CancelAsync(RequestState state, CancellationToken cancellationToken);
}
