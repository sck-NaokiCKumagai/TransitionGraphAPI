using GlobalCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.IO;
using System.Runtime.Loader;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace TransitionGraphDataEdit.Grid;

/// <summary>
/// Production implementation that calls DXC.EngPF.Core.Grid.dll.
///
/// The production DLL type names differ from the dummy DLL, so this class uses reflection and maps
/// results into internal models (GridModels.cs).
/// </summary>
public sealed class RealGridClient : IGridClient
{
    private const int TimeoutSec = 432000;

    private readonly IConfiguration _config;
    private readonly ILogger<RealGridClient>? _logger;

    private static int _assemblyLogOnce;

    public RealGridClient(IConfiguration config, ILogger<RealGridClient>? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger;
    }

    public async Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Trace("GetColumnDataAndRowCountAsync.Param", new { secondCacheId, sessionId });
        var client = CreateProductionClient(secondCacheId, sessionId);
        // Prefer calling the string overload if it exists; otherwise auto-generate the request DTO
        // from the method's parameter type and set SecondCacheId (and PreSecondCacheId if present).
        var taskObj = InvokeWithSecondCacheIdAuto(client, "GetColumnDataAndRowCountAsync", secondCacheId);
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapColumnDataAndRowCount(raw);
    }

    public async Task<GridDataResult> GetGridDataAsync(
        string[] columnIds,
        string gridId,
        string sessionId,
        bool isGetVirtualColumnValue,
        int height,
        int width,
        int x,
        int y,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Trace("GetGridDataAsync.Param", new { gridId, sessionId, columnIds = columnIds ?? Array.Empty<string>(), isGetVirtualColumnValue, height, width, x, y });
        var client = CreateProductionClient(gridId, sessionId);

        // Production DTO: DXC.EngPF.Core.Grid.GridDataParam
        var gridDataParamType = GetTypeOrThrow("DXC.EngPF.Core.Grid.GetGridDataParam");
        var param = Activator.CreateInstance(gridDataParamType)
                   ?? throw new InvalidOperationException("Failed to create GridDataParam.");

        SetProp(param, "GridId", gridId);
        SetProp(param, "X", x);
        SetProp(param, "Y", y);
        SetProp(param, "Width", width);
        SetProp(param, "Height", height);
        SetProp(param, "IsGetVirtualColumnValue", isGetVirtualColumnValue);

        // DisplayColumn: ICollection<string>
        SetProp(param, "DisplayColumn", (columnIds ?? Array.Empty<string>()).ToList());

        var taskObj = Invoke(client, "GetGridDataAsync", new object?[] { param });
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapGridDataResult(raw);
    }

    public async Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, string sessionId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Trace("GetColumnDataAndRowCountAsync.Param", new { secondCacheId, sessionId });
        var client = CreateProductionClient(secondCacheId, sessionId);
        // Prefer calling the string overload if it exists; otherwise auto-generate the request DTO
        // from the method's parameter type and set SecondCacheId (and PreSecondCacheId if present).
        var taskObj = InvokeWithSecondCacheIdAuto(client, "GetGridFilterInfoAsync", secondCacheId);
        var raw = await CastTaskToObject(taskObj).ConfigureAwait(false);
        return MapGridFilterInfoResult(raw);
    }

    public async Task<string> SetFilterAndSortAsync(FilterAndSortParam param, string sessionId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var client = CreateProductionClient(param?.SecondCacheId ?? string.Empty, sessionId);

        // Production DTO: DXC.EngPF.Core.Grid.SetFilterAndSortParam
        var tParam = GetTypeOrThrow("DXC.EngPF.Core.Grid.SetFilterAndSortParam");
        var prodParam = Activator.CreateInstance(tParam)
                       ?? throw new InvalidOperationException("Failed to create SetFilterAndSortParam.");

        SetProp(prodParam, "SecondCacheId", param.SecondCacheId);
        SetProp(prodParam, "SortConditionParameter", param.SortConditionParameter);
        SetProp(prodParam, "IsManualSort", param.IsManualSort);
        SetProp(prodParam, "ManualSortMoveIndex", param.ManualSortMoveIndex);
        SetProp(prodParam, "ManualSortTargetRowIndex", param.ManualSortTargetRowIndex?.ToList() ?? new List<int>());

        var taskObj = Invoke(client, "SetFilterAndSortAsync", new object?[] { prodParam });
        await AwaitVoidTask(taskObj).ConfigureAwait(false);

        return "Accept Request";
    }

    // -----------------

    // Client construction
    // -----------------

    private object CreateProductionClient(string secondCacheId, string sessionId)
    {
        LogAssemblyResolutionOnce();
        // Production client: DXC.EngPF.Core.Grid.Services.GridServiceClient(url, secondCacheId, TimeoutSec)
        // (Namespace in spec: DXC.EngPF.Core.Grid, but actual type is under .Services in the provided DLL.)
        var clientType = FindType("DXC.EngPF.Core.Grid.Services.GridServiceClient")
                         ?? FindType("DXC.EngPF.Core.Grid.GridServiceClient")
                         ?? FindTypeByName("GridServiceClient");

        if (clientType is null)
        {
            var diag = string.Join("\n", BuildGridAssemblyDiagLines());
            throw new NotSupportedException(
                "Production client type 'GridServiceClient' was not found. " +
                "Ensure the correct DXC.EngPF.Core.Grid.dll is deployed.\n" + diag);
        }

        var url = BuildUrl();

        // Constructor signatures may differ between deployments.
// Per current spec we want to prefer: (url, sessionId, timeoutSec).
// Some builds may additionally require secondCacheId (or accept it instead of sessionId),
// so we try a small set of common patterns.
        sessionId ??= string.Empty;
        secondCacheId ??= string.Empty;

        var ctors = new[]
        {
            // Preferred (spec)
            new object?[] { url, sessionId, TimeoutSec },
            new object?[] { url, sessionId },

            // Variants that also include secondCacheId
            new object?[] { url, sessionId, secondCacheId, TimeoutSec },
            new object?[] { url, sessionId, secondCacheId },

            // Legacy patterns (no sessionId)
            new object?[] { url, secondCacheId, TimeoutSec },
            new object?[] { url, secondCacheId },
            new object?[] { url },
        };

        foreach (var argv in ctors)
        {
            try
            {
                return Activator.CreateInstance(clientType, argv)
                       ?? throw new InvalidOperationException("Failed to construct GridServiceClient.");
            }
            catch (MissingMethodException)
            {
                // try next
            }
            catch (Exception ex) when (
                ex is TypeLoadException or FileNotFoundException or FileLoadException or BadImageFormatException
                or TypeInitializationException or TargetInvocationException)
            {
                try
                {
                    WriteDiagFile(new[]
                    {
                        "[GridDLL] ERROR while constructing GridServiceClient:",
                        "[GridDLL]   - ExceptionType=" + ex.GetType().FullName,
                        "[GridDLL]   - " + ex,
                        "[GridDLL]   - Inner=" + (ex.InnerException?.ToString() ?? "(none)"),
                    });
                    TryLogAssemblyReferences(clientType.Assembly);
                }
                catch { /* ignore */ }
                throw;
            }
        }

        throw new MissingMethodException(
            clientType.FullName,
            "No supported constructor found. Tried: (string url, string secondCacheId, int timeoutSec), (string url, string secondCacheId), (string url)"
        );
    }

    /*****
    private string BuildUrl()
    {
        try
        {
            // CacheがDictionary/ConcurrentDictionary等で列挙できる場合
            var keys = GlobalCache.Cache.Keys
                .Where(k => k.Contains("100") || k.Contains("Request") || k.Contains("parameter", StringComparison.OrdinalIgnoreCase))
                .Take(200)
                .ToArray();

            WriteDiagFile(new[]
            {
                $"[GridApi] CacheKeys(hit)={string.Join(", ", keys)}"
            });
        }
        catch (Exception ex)
        {
            WriteDiagFile(new[] { $"[GridApi] CacheKeys(dump) failed: {ex.GetType().Name}: {ex.Message}" });
        }
        // (Spec) RequestId=100 parameter[0] => server base URL; fallback 127.0.0.1.
        var baseUrl =
            GlobalCache.Cache.Get<string>("RequestId100.Parameter0")
            ?? GlobalCache.Cache.Get<string>("RequestId100:parameter0")
            ?? GlobalCache.Cache.Get<string>("Request100.Parameter0")
            ?? _config.GetValue<string>("GridDll:ServerBaseUrl")
            ?? "127.0.0.1";

        baseUrl = NormalizeBaseUrl(baseUrl);

        var rootDir = _config.GetValue<string>("GridRootDir:RootDir") ?? string.Empty;
        rootDir = NormalizeRootDir(rootDir);

        var raw100 = GlobalCache.Cache.Get<string>("RequestId100.Parameter0")
                     ?? GlobalCache.Cache.Get<string>("RequestId100:parameter0")
                     ?? GlobalCache.Cache.Get<string>("Request100.Parameter0");

        WriteDiagFile(new[]
        {
            $"[GridApi] RequestId100(raw)={raw100 ?? "(null)"}",
            $"[GridApi] ServerBaseUrl(appsettings)={_config.GetValue<string>("GridDll:ServerBaseUrl") ?? "(null)"}",
            $"[GridApi] GridRootDir={_config.GetValue<string>("GridRootDir:RootDir") ?? "(null)"}",
        });

        return $"{baseUrl}{rootDir}/services/grid/1_6_0_0/";
    }
    *****/
    private string BuildUrl()
    {
        // (Spec) RequestId=100 parameter[0] => server base URL; fallback 127.0.0.1.
        var baseUrl =
            GlobalCache.Cache.Get<string>("RequestId100.Parameter0")
            ?? GlobalCache.Cache.Get<string>("RequestId100:parameter0")
            ?? GlobalCache.Cache.Get<string>("Request100.Parameter0")
            ?? _config.GetValue<string>("GridDll:ServerBaseUrl")
            ?? "127.0.0.1";

        baseUrl = NormalizeBaseUrl(baseUrl);

        var rootDir = _config.GetValue<string>("GridRootDir:RootDir") ?? string.Empty;
        rootDir = NormalizeRootDir(rootDir);

        var raw100 = GlobalCache.Cache.Get<string>("RequestId100.Parameter0")
                     ?? GlobalCache.Cache.Get<string>("RequestId100:parameter0")
                     ?? GlobalCache.Cache.Get<string>("Request100.Parameter0");

        WriteDiagFile(new[]
        {
	        $"[GridApi] RequestId100(raw)={raw100 ?? "(null)"}",
	        $"[GridApi] ServerBaseUrl(appsettings)={_config.GetValue<string>("GridDll:ServerBaseUrl") ?? "(null)"}",
	        $"[GridApi] GridRootDir={_config.GetValue<string>("GridRootDir:RootDir") ?? "(null)"}",
	    });

        var built = $"{baseUrl}/EngPF_t/services/grid/1_6_0_0/";
        //var built = $"{baseUrl}/EngPF_t/services/grid/1_6_0_0/api/GridRead/get-column-data-and-row-count/";

        WriteDiagFile(new[] { $"[GridApi] BuiltUrl={built}" }); // 最終URLを必ず出す
        return built;
    }

    private static string NormalizeBaseUrl(string baseUrl)
    {
        baseUrl = baseUrl.Trim();

        if (!baseUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
            !baseUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            // ローカルだけ http、それ以外は https をデフォルトにする
            var isLocal =
                baseUrl.StartsWith("127.0.0.1", StringComparison.OrdinalIgnoreCase) ||
                baseUrl.StartsWith("localhost", StringComparison.OrdinalIgnoreCase);

            baseUrl = (isLocal ? "http://" : "https://") + baseUrl;
        }

        return baseUrl.TrimEnd('/');
    }

    private static string NormalizeRootDir(string rootDir)
    {
        rootDir = (rootDir ?? string.Empty).Trim();
        if (rootDir.Length == 0) return string.Empty;
        if (!rootDir.StartsWith("/", StringComparison.Ordinal)) rootDir = "/" + rootDir;
        return rootDir.TrimEnd('/');
    }

    // -----------------
    // Mapping
    // -----------------

    private static ColumnDataAndRowCountResult MapColumnDataAndRowCount(object raw)
    {
        var outObj = new ColumnDataAndRowCountResult();
        var t = raw.GetType();

        outObj.RowCount = (int)(t.GetProperty("RowCount")?.GetValue(raw) ?? 0);

        var colArrayObj = t.GetProperty("ColumnDataArray")?.GetValue(raw)
                          ?? t.GetProperty("ColumnDataList")?.GetValue(raw);

        if (colArrayObj is System.Collections.IEnumerable en)
        {
            foreach (var item in en)
            {
                if (item == null) continue;
                outObj.ColumnDataArray.Add(MapDataItemColumnData(item));
            }
        }

        return outObj;
    }

    private static DataItemColumnData MapDataItemColumnData(object raw)
    {
        var t = raw.GetType();
        return new DataItemColumnData
        {
            ColumnId = (string?)t.GetProperty("ColumnId")?.GetValue(raw) ?? string.Empty,
            DataItemKind = (string?)t.GetProperty("DataItemKind")?.GetValue(raw),
            DisplayString = (string?)t.GetProperty("DisplayString")?.GetValue(raw),
            IsHeader = (bool?)(t.GetProperty("IsHeader")?.GetValue(raw)) ?? false,
            IsNnumeric = (bool?)(t.GetProperty("IsNnumeric")?.GetValue(raw)) ?? false,
        };
    }

    private static GridDataResult MapGridDataResult(object raw)
    {
        // Production type name in this DLL is "GetGridDataResult" but we map by properties.
        var t = raw.GetType();
        var outObj = new GridDataResult();

        var gridDataObj = t.GetProperty("GridData")?.GetValue(raw);
        if (gridDataObj is System.Collections.IEnumerable cols)
        {
            foreach (var col in cols)
            {
                if (col is System.Collections.IEnumerable rows)
                {
                    var list = new List<object?>();
                    foreach (var cell in rows) list.Add(cell);
                    outObj.GridData.Add(list);
                }
            }
        }

        var idxObj = t.GetProperty("ColumnIndexArray")?.GetValue(raw);
        if (idxObj is System.Collections.IEnumerable idxEn)
        {
            foreach (var v in idxEn)
            {
                if (v == null) continue;
                if (v is int i) outObj.ColumnIndexArray.Add(i);
                else if (int.TryParse(v.ToString(), out var ii)) outObj.ColumnIndexArray.Add(ii);
            }
        }

        return outObj;
    }

    private static GridFilterInfoResult MapGridFilterInfoResult(object raw)
    {
        // Production type name in this DLL is "GetGridFilterInfoResult" but we map by properties.
        var t = raw.GetType();
        var outObj = new GridFilterInfoResult();

        var listObj = t.GetProperty("FilterInfoList")?.GetValue(raw);
        if (listObj is System.Collections.IEnumerable en)
        {
            foreach (var item in en)
            {
                if (item == null) continue;
                var it = item.GetType();
                outObj.FilterInfoList.Add(new FilterInfoString
                {
                    ColumnName = (string?)it.GetProperty("ColumnName")?.GetValue(item),
                    FilterString = (string?)it.GetProperty("FilterString")?.GetValue(item),
                    IsCurrentFilter = (bool?)(it.GetProperty("IsCurrentFilter")?.GetValue(item)) ?? false,
                });
            }
        }

        return outObj;
    }

    

// -----------------
// Trace logging helpers
// -----------------
private static string TracePath =>
    Environment.GetEnvironmentVariable("TG_GRID_TRACE_PATH")
    ?? @"C:\temp\tg_grid_trace.jsonl";

private static readonly JsonSerializerOptions TraceJsonOptions = new()
{
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
};

private static void Trace(string kind, object payload)
{
    try
    {
        var line = JsonSerializer.Serialize(new
        {
            ts = DateTimeOffset.Now.ToString("O"),
            kind,
            payload
        }, TraceJsonOptions);
        Directory.CreateDirectory(Path.GetDirectoryName(TracePath) ?? ".");
        File.AppendAllText(TracePath, line + Environment.NewLine);
    }
    catch
    {
        // never fail the main flow due to tracing
    }
}

private static object Simplify(object? obj, int depth = 0)
{
    if (obj is null) return null!;
    if (depth > 3) return obj.ToString() ?? string.Empty;
    var t = obj.GetType();
    if (t.IsPrimitive || obj is string || obj is decimal) return obj;

    if (obj is System.Collections.IEnumerable en && obj is not string)
    {
        var list = new List<object?>();
        foreach (var it in en)
        {
            list.Add(Simplify(it, depth + 1));
            if (list.Count > 2000) break;
        }
        return list;
    }

    var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);
    var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    foreach (var p in props)
    {
        if (!p.CanRead) continue;
        if (p.GetIndexParameters().Length != 0) continue;
        var name = p.Name;
        if (name.EndsWith("Bytes", StringComparison.OrdinalIgnoreCase)) continue;
        try { dict[name] = Simplify(p.GetValue(obj), depth + 1); }
        catch { dict[name] = "(unreadable)"; }
    }
    return dict;
}

// -----------------
    // Reflection helpers
    // -----------------

    private static object Invoke(object instance, string methodName, object?[] args)
    {
        if (instance is null) throw new ArgumentNullException(nameof(instance));
        if (methodName is null) throw new ArgumentNullException(nameof(methodName));
        args ??= Array.Empty<object?>();

        var type = instance.GetType();

        var all = type.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                      .Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal))
                      .ToArray();

        if (all.Length == 0)
            throw new MissingMethodException(type.FullName, methodName);

        var candidates = all.Where(m => m.GetParameters().Length == args.Length).ToArray();
        if (candidates.Length == 0)
            throw new MissingMethodException(type.FullName, $"{methodName} (arity={args.Length})");

        var scored = new List<(MethodInfo Method, int Score)>(candidates.Length);

        foreach (var m in candidates)
        {
            var ps = m.GetParameters();
            var score = 0;
            var ok = true;

            for (int i = 0; i < ps.Length; i++)
            {
                if (!TryScoreArgument(args[i], ps[i].ParameterType, out var s))
                {
                    ok = false;
                    break;
                }
                score += s;
            }

            if (ok) scored.Add((m, score));
        }

        if (scored.Count == 0)
        {
            var sigs = string.Join(Environment.NewLine, candidates.Select(m => $"  - {FormatSignature(type, m)}"));
            throw new MissingMethodException(
                type.FullName,
                $"{methodName}: no overload matches the given arguments.{Environment.NewLine}Candidates:{Environment.NewLine}{sigs}");
        }

        var bestScore = scored.Min(x => x.Score);
        var best = scored.Where(x => x.Score == bestScore).Select(x => x.Method).ToArray();

        if (best.Length > 1)
        {
            var sigs = string.Join(Environment.NewLine, best.Select(m => $"  - {FormatSignature(type, m)}"));
            throw new AmbiguousMatchException(
                $"{type.FullName}.{methodName}: ambiguous overload resolution (score={bestScore}).{Environment.NewLine}Matches:{Environment.NewLine}{sigs}");
        }

        var target = best[0];

        try
        {
            return target.Invoke(instance, args) ?? throw new InvalidOperationException($"{methodName} returned null.");
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            // Unwrap reflection invocation exceptions so callers see the real root cause.
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
            throw; // unreachable
        }
    }

    /// <summary>
    /// Calls a method that takes either (string secondCacheId) or (SomeParam param).
    /// If the string overload is not found, this method automatically creates the parameter object
    /// using the actual MethodInfo parameter type (avoids AssemblyLoadContext type mismatches)
    /// and sets SecondCacheId and PreSecondCacheId (if present).
    /// </summary>
    private static object InvokeWithSecondCacheIdAuto(object client, string methodName, string secondCacheId)
    {
        // 1) Try string overload first (supports older / different DLLs)
        try
        {
            return Invoke(client, methodName, new object?[] { secondCacheId });
        }
        catch (MissingMethodException)
        {
            // 2) Fallback: auto-create request DTO using the method's actual parameter type.
            return InvokeByAutoParam(
                client,
                methodName,
                new Dictionary<string, object?>
                {
                    ["SecondCacheId"] = secondCacheId,
                    // Some versions expect this property; if absent it will be ignored.
                    ["PreSecondCacheId"] = string.Empty,
                });
        }
    }

    private static object InvokeByAutoParam(object client, string methodName, Dictionary<string, object?> propValues)
    {
        var tClient = client.GetType();

        var methods = tClient.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                             .Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal))
                             .ToArray();

        if (methods.Length == 0)
            throw new MissingMethodException(tClient.FullName, methodName);

        // Prefer single-argument non-string class parameter overloads.
        var candidates = methods
            .Where(m => m.GetParameters().Length == 1)
            .Select(m => new { Method = m, ParamType = m.GetParameters()[0].ParameterType })
            .Where(x => x.ParamType.IsClass && x.ParamType != typeof(string))
            .ToArray();

        if (candidates.Length == 0)
        {
            var sigs = string.Join(Environment.NewLine, methods.Select(m => $"  - {FormatSignature(tClient, m)}"));
            throw new MissingMethodException(
                tClient.FullName,
                $"{methodName}: no 1-arg object parameter overload exists.{Environment.NewLine}Methods:{Environment.NewLine}{sigs}");
        }

        // Choose the param type that can accept the most of the provided properties.
        var scored = candidates
            .Select(x => new
            {
                x.Method,
                x.ParamType,
                MatchCount = CountSettableProps(x.ParamType, propValues.Keys)
            })
            .OrderByDescending(x => x.MatchCount)
            .ToArray();

        if (scored[0].MatchCount == 0)
        {
            var sigs = string.Join(Environment.NewLine, candidates.Select(m => $"  - {FormatSignature(tClient, m.Method)}"));
            throw new MissingMethodException(
                tClient.FullName,
                $"{methodName}: could not find a parameter type with expected properties.{Environment.NewLine}Candidates:{Environment.NewLine}{sigs}");
        }

        var best = scored[0];

        // IMPORTANT: create the instance from the MethodInfo parameter type itself
        // (prevents 'same name different type' issues across AssemblyLoadContexts).
        var param = Activator.CreateInstance(best.ParamType)
                   ?? throw new InvalidOperationException($"Failed to create param instance: {best.ParamType.FullName}");

        foreach (var kv in propValues)
        {
            TrySetPropIfExists(param, kv.Key, kv.Value);
        }

        return Invoke(client, methodName, new object?[] { param });
    }

    private static int CountSettableProps(Type t, IEnumerable<string> propNames)
    {
        int c = 0;
        foreach (var name in propNames)
        {
            var p = t.GetProperty(name, BindingFlags.Instance | BindingFlags.Public);
            if (p != null && p.CanWrite) c++;
        }
        return c;
    }

    private static void TrySetPropIfExists(object obj, string propName, object? value)
    {
        var p = obj.GetType().GetProperty(propName, BindingFlags.Instance | BindingFlags.Public);
        if (p == null || !p.CanWrite) return;

        // Handle collection assignment flexibly.
        if (value is List<string> sList && p.PropertyType.IsAssignableFrom(typeof(List<string>)))
        {
            p.SetValue(obj, sList);
            return;
        }
        if (value is List<int> iList && p.PropertyType.IsAssignableFrom(typeof(List<int>)))
        {
            p.SetValue(obj, iList);
            return;
        }

        p.SetValue(obj, value);
    }

    private static bool TryScoreArgument(object? arg, Type parameterType, out int score)
    {
        score = 0;

        if (parameterType.IsByRef) parameterType = parameterType.GetElementType()!;

        if (arg is null)
        {
            if (!IsNullable(parameterType)) return false;
            score = 50 - Math.Min(GetInheritanceDepth(parameterType), 40);
            return true;
        }

        var argType = arg.GetType();

        if (parameterType == argType)
        {
            score = 0;
            return true;
        }

        var underlying = Nullable.GetUnderlyingType(parameterType);
        if (underlying != null)
        {
            if (underlying == argType)
            {
                score = 1;
                return true;
            }
            parameterType = underlying;
        }

        if (parameterType.IsAssignableFrom(argType))
        {
            score = 10 + GetTypeDistance(argType, parameterType);
            return true;
        }

        return false;
    }

    private static bool IsNullable(Type t)
        => !t.IsValueType || Nullable.GetUnderlyingType(t) != null;

    private static int GetInheritanceDepth(Type t)
    {
        if (t.IsInterface) return 1;
        int depth = 0;
        var cur = t;
        while (cur.BaseType != null)
        {
            depth++;
            cur = cur.BaseType;
        }
        return depth;
    }

    private static int GetTypeDistance(Type argType, Type targetType)
    {
        if (argType == targetType) return 0;

        if (targetType.IsInterface)
            return argType.GetInterfaces().Contains(targetType) ? 5 : 1000;

        int d = 0;
        var cur = argType;
        while (cur != null)
        {
            if (cur == targetType) return d;
            cur = cur.BaseType!;
            d++;
        }
        return 1000;
    }

    private static string FormatSignature(Type declaringType, MethodInfo m)
    {
        var ps = m.GetParameters();
        var p = string.Join(", ", ps.Select(x => x.ParameterType.FullName ?? x.ParameterType.Name));
        return $"{declaringType.FullName}.{m.Name}({p})";
    }

    private static async Task<object> CastTaskToObject(object taskObj)
    {
        try
        {
            if (taskObj is not Task task)
                throw new InvalidOperationException("Expected a Task return type.");

            await task.ConfigureAwait(false);

            var t = taskObj.GetType();
            if (!t.IsGenericType) return new object();

            var resultProp = t.GetProperty("Result");
            return resultProp?.GetValue(taskObj) ?? new object();
        }
        catch (DXC.EngPF.Core.Grid.ApiException aex)
        {
            var body = aex.Response ?? "";
            if (body.Length > 2000) body = body.Substring(0, 2000);

            WriteDiagFile(new[]
            {
                $"[GridApi] Status={aex.StatusCode}",
                $"[GridApi] ResponseHead={body}",
            });
            throw;
        }
    }

    private static async Task AwaitVoidTask(object taskObj)
    {
        if (taskObj is Task t)
        {
            await t.ConfigureAwait(false);
            return;
        }
        throw new InvalidCastException($"Object is not a Task: {taskObj.GetType().FullName}");
    }

    private static void SetProp(object obj, string propName, object? value)
    {
        var p = obj.GetType().GetProperty(propName, BindingFlags.Instance | BindingFlags.Public);
        if (p is null)
            throw new MissingMemberException(obj.GetType().FullName, propName);

        // Handle collection assignment flexibly.
        if (value is List<string> sList && p.PropertyType.IsAssignableFrom(typeof(List<string>)))
        {
            p.SetValue(obj, sList);
            return;
        }
        if (value is List<int> iList && p.PropertyType.IsAssignableFrom(typeof(List<int>)))
        {
            p.SetValue(obj, iList);
            return;
        }

        p.SetValue(obj, value);
    }

    private void LogAssemblyResolutionOnce()
    {
        if (Interlocked.Exchange(ref _assemblyLogOnce, 1) != 0) return;

        try
        {
            foreach (var line in BuildGridAssemblyDiagLines())
            {
                // 1) logger (preferred) 2) stderr (always shows up in wrapper logs even if ILogger filters)
                if (_logger != null) _logger.LogInformation(line);
                Console.Error.WriteLine(line);
            }
        }
        catch
        {
            // never fail the main flow due to logging
        }
    }

    /*****
    private IEnumerable<string> BuildGridAssemblyDiagLines()
    {
        var baseDir = AppContext.BaseDirectory;
        var p1 = Path.Combine(baseDir, "DXC.EngPF.Core.Grid.dll");
        var p2 = Path.Combine(baseDir, "DLL", "DXC.EngPF.Core.Grid.dll");

        yield return $"[GridDLL] BaseDir={baseDir}";
        yield return $"[GridDLL] Candidate1={p1} exists={File.Exists(p1)}";
        yield return $"[GridDLL] Candidate2={p2} exists={File.Exists(p2)}";

        var loaded = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => string.Equals(a.GetName().Name, "DXC.EngPF.Core.Grid", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (loaded.Length == 0)
        {
            yield return "[GridDLL] LoadedAssemblies: (none)";
            yield break;
        }

        yield return "[GridDLL] LoadedAssemblies:";
        foreach (var a in loaded)
        {
            yield return $"[GridDLL]   - {a.FullName} | Location={(SafeLocation(a) ?? "(null)")}";
        }
    }
    *****/
    private IEnumerable<string> BuildGridAssemblyDiagLines()
    {
        var baseDir = AppContext.BaseDirectory;

        // ---- Process / log4net diagnostics (DataEdit side) ----
        yield return $"[Proc] Is64BitProcess={Environment.Is64BitProcess}";

        var l1 = Path.Combine(baseDir, "log4net.dll");
        var l2 = Path.Combine(baseDir, "DLL", "log4net.dll");
        yield return $"[log4net] File={l1} Machine={PeMachine(l1)}";
        yield return $"[log4net] File={l2} Machine={PeMachine(l2)}";
        // -------------------------------------------------------

        var p1 = Path.Combine(baseDir, "DXC.EngPF.Core.Grid.dll");
        var p2 = Path.Combine(baseDir, "DLL", "DXC.EngPF.Core.Grid.dll");

        yield return $"[GridDLL] BaseDir={baseDir}";
        yield return $"[GridDLL] Candidate1={p1} exists={File.Exists(p1)}";
        yield return $"[GridDLL] Candidate2={p2} exists={File.Exists(p2)}";

        var loaded = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => string.Equals(a.GetName().Name, "DXC.EngPF.Core.Grid", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (loaded.Length == 0)
        {
            yield return "[GridDLL] LoadedAssemblies: (none)";
            yield break;
        }

        yield return "[GridDLL] LoadedAssemblies:";
        foreach (var a in loaded)
        {
            yield return $"[GridDLL]   - {a.FullName} | Location={(SafeLocation(a) ?? "(null)")}";
        }
    }


    private static string? SafeLocation(Assembly a)
    {
        try { return a.Location; } catch { return null; }
    }

    private static string PeMachine(string path)
    {
        try
        {
            if (!File.Exists(path)) return "NOT_FOUND";

            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var br = new BinaryReader(fs);

            // "MZ"
            if (br.ReadUInt16() != 0x5A4D) return "NOT_PE";

            fs.Seek(0x3C, SeekOrigin.Begin);
            var lfanew = br.ReadInt32();

            fs.Seek(lfanew, SeekOrigin.Begin);
            // "PE\0\0"
            if (br.ReadUInt32() != 0x00004550) return "NOT_PE";

            var machine = br.ReadUInt16();
            return machine switch
            {
                0x014c => "x86 (IMAGE_FILE_MACHINE_I386)",
                0x8664 => "x64 (IMAGE_FILE_MACHINE_AMD64)",
                0xAA64 => "ARM64 (IMAGE_FILE_MACHINE_ARM64)",
                _ => $"Unknown(0x{machine:X4})"
            };
        }
        catch (Exception ex)
        {
            return "ERR:" + ex.GetType().Name;
        }
    }

    private static Type GetTypeOrThrow(string fullName)
        => FindType(fullName) ?? throw new TypeLoadException($"Type not found: {fullName}");

    private static Type? FindType(string fullName)
    {
        // NOTE:
        // The DataEdit process stdout/stderr may not be captured by the wrapper.
        // To make troubleshooting reliable on real machines, we also write diagnostics
        // to a local file under the process base directory.

        // 1) Search already-loaded assemblies first (fast path).
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var t = asm.GetType(fullName, throwOnError: false, ignoreCase: false);
                if (t != null) return t;
            }
            catch
            {
                // Some dynamic/reflection-only assemblies can throw; ignore and continue.
            }
        }

        // 2) Ensure DXC.EngPF.Core.Grid is loaded from the deployed folder.
        //    IMPORTANT: Assembly.Load(AssemblyName) can pick up an unexpected version from elsewhere.
        //    We search both the app base directory and the conventional "DLL" subfolder because
        //    this repository deploys vendor DLLs under ./DLL.
        try
        {
            var loaded = AppDomain.CurrentDomain.GetAssemblies()
                .FirstOrDefault(a => string.Equals(a.GetName().Name, "DXC.EngPF.Core.Grid", StringComparison.OrdinalIgnoreCase));

            if (loaded is null)
            {
                var baseDir = AppContext.BaseDirectory;

                try
                {
                    WriteDiagFile(new[]
                    {
                        "[GridDLL] FindType: need load DXC.EngPF.Core.Grid",
                        $"[GridDLL] BaseDir={baseDir}",
                        $"[Proc] Is64BitProcess={Environment.Is64BitProcess}",
                        $"[log4net] File={Path.Combine(baseDir, "log4net.dll")} Machine={PeMachine(Path.Combine(baseDir, "log4net.dll"))}",
                        $"[log4net] File={Path.Combine(baseDir, "DLL", "log4net.dll")} Machine={PeMachine(Path.Combine(baseDir, "DLL", "log4net.dll"))}",
                    });
                }
                catch { /* ignore */ }

                // Prefer explicitly deployed DLLs.
                var candidatePaths = new[]
                {
                    Path.Combine(baseDir, "DXC.EngPF.Core.Grid.dll"),
                    Path.Combine(baseDir, "DLL", "DXC.EngPF.Core.Grid.dll"),
                };

                try
                {
                    WriteDiagFile(candidatePaths.Select(p => $"[GridDLL] Candidate={p} exists={File.Exists(p)}").ToArray());
                }
                catch { /* ignore */ }

                var dllPath = candidatePaths.FirstOrDefault(File.Exists);

                if (dllPath != null)
                {
                    var full = Path.GetFullPath(dllPath);
                    try { Console.WriteLine($"[GridDLL] Loading DXC.EngPF.Core.Grid from {full}"); } catch { }
                    try
                    {
                        loaded = AssemblyLoadContext.Default.LoadFromAssemblyPath(full);
                    }
                    catch (Exception ex)
                    {
                        try { WriteDiagFile(new[] { "[GridDLL] LoadFromAssemblyPath FAILED: " + ex }); } catch { }
                        throw;
                    }
                    try { Console.WriteLine($"[GridDLL] Loaded: {loaded.FullName} | Location={SafeLocation(loaded) ?? "(null)"}"); } catch { }

                    try
                    {
                        WriteDiagFile(new[]
                        {
                            $"[GridDLL] Loaded: {loaded.FullName}",
                            $"[GridDLL] Location={SafeLocation(loaded) ?? "(null)"}",
                        });
                    }
                    catch { /* ignore */ }
                }
                else
                {
                    // Avoid loading an unexpected assembly from elsewhere.
                    // If the vendor DLL is not deployed next to the executable, treat it as a deployment error.
                    try
                    {
                        WriteDiagFile(new[]
                        {
                            "[GridDLL] ERROR: DXC.EngPF.Core.Grid.dll not found in expected locations.",
                            "[GridDLL] Searched:",
                            $"[GridDLL]   - {candidatePaths[0]}",
                            $"[GridDLL]   - {candidatePaths[1]}",
                        });
                    }
                    catch { /* ignore */ }

                    return null;
                }
            }

            try
            {
                // Try resolving the type. If the type exists but cannot be loaded due to missing dependencies,
                // this can throw TypeLoadException.
                return loaded.GetType(fullName, throwOnError: false, ignoreCase: false);
            }
            catch (TypeLoadException ex)
            {
                try
                {
                    WriteDiagFile(new[]
                    {
                        "[GridDLL] TypeLoadException while resolving type: " + fullName,
                        "[GridDLL]   - " + ex,
                    });
                    TryLogAssemblyReferences(loaded);
                }
                catch { /* ignore */ }
                return null;
            }
        }
        catch (Exception ex)
        {
            try { WriteDiagFile(new[] { "[GridDLL] FindType failed: " + ex }); } catch { }
            return null;
        }
    }
    private static void TryLogAssemblyReferences(Assembly asm)
    {
        try
        {
            var baseDir = AppContext.BaseDirectory;

            // List referenced assemblies and whether their DLL exists next to the executable.
            var refs = asm.GetReferencedAssemblies();
            var lines = new List<string>
            {
                "[GridDLL] ReferencedAssemblies from " + (asm.FullName ?? "(null)"),
                "[GridDLL]   Location=" + (SafeLocation(asm) ?? "(null)"),
            };

            foreach (var r in refs)
            {
                var dllName = r.Name + ".dll";
                var p1 = Path.Combine(baseDir, dllName);
                var p2 = Path.Combine(baseDir, "DLL", dllName);

                lines.Add($"[GridDLL]   - {r.FullName}");
                lines.Add($"[GridDLL]       exists(BaseDir)={File.Exists(p1)} : {p1}");
                lines.Add($"[GridDLL]       exists(BaseDir\\DLL)={File.Exists(p2)} : {p2}");
            }

            WriteDiagFile(lines);
        }
        catch (Exception ex)
        {
            try { WriteDiagFile(new[] { "[GridDLL] TryLogAssemblyReferences failed: " + ex }); } catch { }
        }
    }



    private static void WriteDiagFile(IEnumerable<string> lines)
    {
        try
        {
            var baseDir = AppContext.BaseDirectory;
            var path = Path.Combine(baseDir, "grid_diag.log");
            var stamp = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.fff");
            using var sw = new StreamWriter(path, append: true);
            foreach (var line in lines)
                sw.WriteLine($"[{stamp}] {line}");
        }
        catch
        {
            // ignore
        }
    }

    // Optional: a safer type search by short name (handles nesting like GridService+GridServiceClient)
    private static Type? FindTypeByName(string shortName)
    {
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                Type[] types;
                try
                {
                    types = asm.GetTypes();
                }
                catch (ReflectionTypeLoadException rtle)
                {
                    types = rtle.Types.Where(t => t != null).Cast<Type>().ToArray();
                }

                var t = types.FirstOrDefault(t => string.Equals(t.Name, shortName, StringComparison.Ordinal));
                if (t != null) return t;
            }
            catch
            {
                // ignore and continue
            }
        }
        return null;
    }
}

