using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace TransitionGraphDataEdit;

/// <summary>
/// Cache key builder for RequestKey(=RequestId) 4-6.
///
/// Spec (10-1) describes a hierarchical key (Lot -> Process/Time -> Item/Wafer).
/// For the current phase we generate deterministic hashes from the request payload
/// (lot list / process / start-end / item list / wafer list) and build one final key.
///
/// This implementation is intentionally tolerant to different client encodings:
/// - JSON array string: ["A","B"]
/// - CSV string: A,B
/// - single value string
///
/// Missing fields are treated as empty.
/// </summary>
internal static class CacheKeyBuilder
{
    /// <summary>
    /// Build cache key for RequestId 4-6.
    ///
    /// Spec (7): cache is keyed by "GUID + parameters".
    /// Spec (10-1): parameters are summarized into deterministic hashes.
    /// </summary>
    public static string BuildForDataSet(string clientGuid, string requestKey, IReadOnlyDictionary<string, string> parameters)
    {
        // A: lot name list
        var lots = ParseList(FirstValue(parameters, "lotNames", "lotNameList", "lots", "parm2"));

        // B: process
        var process = FirstValue(parameters, "process", "parm1") ?? "";

        // C/D: start/end
        var start = FirstValue(parameters, "startTime", "start", "parm2") ?? "";
        var end = FirstValue(parameters, "endTime", "end", "parm3") ?? "";

        // E: test name list (DB column: TEST_NAME)
        var testNames = ParseList(FirstValue(parameters, "testNames", "testNameList", "tests", "parm3"));

        // F: wafer name list (optional)
        var wafers = ParseList(FirstValue(parameters, "waferNames", "waferNameList", "wafers"));

        // G: —±“xiLot/Waferj
        var grain = FirstValue(parameters, "granularity", "grain", "dataSort", "Parm4", "parm4") ?? "";
        grain = grain.Trim();
        var grainHash = Sha256(grain);

        // Also support lot-item dictionary passed as JSON for RequestKey objects.
        // This is optional and used only to improve cache hit accuracy.
        var reqKeyJson = parameters.TryGetValue("reqKeyJson", out var rk) ? rk : null;
        var reqKeyHash = string.IsNullOrWhiteSpace(reqKeyJson) ? "" : Sha256(reqKeyJson!);

        // Hash components (sorted for determinism).
        var lotHash = Sha256(string.Join("|", lots.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)));
        var itemHash = Sha256(string.Join("|", testNames.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)));
        var waferHash = Sha256(string.Join("|", wafers.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)));
        var procHash = Sha256(process);
        var timeHash = Sha256($"{start}__{end}");

        // Final cache key
        // Example: TG:GUID=<guid>:RK=5:LOT=<hash>:P=<hash>:T=<hash>:I=<hash>:W=<hash>:K=<hash>
        // NOTE: clientGuid may be empty for backward-compat. When empty, the key becomes shared.
        var g = string.IsNullOrWhiteSpace(clientGuid) ? "" : $"GUID={clientGuid}:";
        //return $"TG:{g}RK={requestKey}:LOT={lotHash}:P={procHash}:T={timeHash}:I={itemHash}:W={waferHash}:K={reqKeyHash}";
        return $"TG:{g}RK={requestKey}:G={grainHash}:LOT={lotHash}:P={procHash}:T={timeHash}:I={itemHash}:W={waferHash}:K={reqKeyHash}";
    }

    // Backward compatible overload (legacy callers that didn't include client GUID).
    public static string BuildForDataSet(string requestKey, IReadOnlyDictionary<string, string> parameters)
        => BuildForDataSet(clientGuid: "", requestKey, parameters);

    private static string? FirstValue(IReadOnlyDictionary<string, string> parameters, params string[] keys)
    {
        foreach (var k in keys)
        {
            // try exact
            if (parameters.TryGetValue(k, out var v))
                return v;

            // try case-insensitive scan (client may send different casing)
            foreach (var p in parameters)
            {
                if (string.Equals(p.Key, k, StringComparison.OrdinalIgnoreCase))
                    return p.Value;
            }
        }
        return null;
    }

    private static List<string> ParseList(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return new List<string>();

        raw = raw.Trim();

        // JSON array
        if (raw.StartsWith("[") && raw.EndsWith("]"))
        {
            try
            {
                var xs = JsonSerializer.Deserialize<List<string>>(raw);
                return xs?.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).ToList()
                       ?? new List<string>();
            }
            catch
            {
                // fall-through to CSV parsing
            }
        }

        // CSV / single
        var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length > 0)
            return parts.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).ToList();

        return new List<string>();
    }

    private static string Sha256(string input)
    {
        using var sha = SHA256.Create();
        var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes);
    }
}

internal sealed record CachedResultMeta(string ResultId, int TotalCount);