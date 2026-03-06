using GlobalCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;

namespace TransitionGraphAlignment.Grid;

/// <summary>
/// Production implementation that calls DXC.EngPF.Core.Grid.dll.
///
/// This worker (Alignment) must behave the same as DataEdit for RequestId=8/9.
/// - RequestId=100 provides the other-system server base address (may be IP).
/// - The server base address is stored in GlobalCache with the unified key "RequestId100.Parameter0".
/// - This client reads that cache and builds the Grid URL using appsettings GridRootDir.
/// </summary>
public sealed class RealGridClient : IGridClient
{
    private const int TimeoutSec = 432000;

    private readonly IConfiguration _config;
    private readonly ILogger<RealGridClient>? _logger;

    // -----------------
    // Trace dump (for cross-team investigation)
    //  - JSONL file at C:\temp\tg_grid_trace.jsonl by default
    //  - Override path by env var: TG_GRID_TRACE_PATH
    // -----------------

    private static readonly string TracePath =
        Environment.GetEnvironmentVariable("TG_GRID_TRACE_PATH") ?? @"C:\temp\tg_grid_trace.jsonl";

    private static void Trace(string kind, object payload)
    {
        try
        {
            var dir = Path.GetDirectoryName(TracePath);
            if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);

            var lineObj = new
            {
                ts = DateTimeOffset.Now.ToString("o"),
                kind,
                payload
            };

            var json = JsonSerializer.Serialize(lineObj, new JsonSerializerOptions
            {
                WriteIndented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });
            File.AppendAllText(TracePath, json + Environment.NewLine, Encoding.UTF8);
        }
        catch
        {
            // Never break main flow due to tracing.
        }
    }

    private static object? Simplify(object? obj, int depth = 0)
    {
        if (obj is null) return null;
        if (depth > 6) return obj.ToString();

        var t = obj.GetType();
        if (t.IsPrimitive || obj is string || obj is decimal || obj is DateTime || obj is DateTimeOffset || obj is Guid)
            return obj;

        if (obj is System.Collections.IEnumerable enumerable && obj is not string)
        {
            var list = new List<object?>();
            var i = 0;
            foreach (var it in enumerable)
            {
                if (i++ >= 2000) break;
                list.Add(Simplify(it, depth + 1));
            }
            return list;
        }

        try
        {
            var dict = new Dictionary<string, object?>();
            foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!p.CanRead) continue;
                object? v;
                try { v = p.GetValue(obj); } catch { v = "(get_failed)"; }
                dict[p.Name] = Simplify(v, depth + 1);
            }
            return dict;
        }
        catch
        {
            return obj.ToString();
        }
    }

    private static object? TryGetProp(object obj, string name)
    {
        try
        {
            var p = obj.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance);
            return p?.CanRead == true ? p.GetValue(obj) : null;
        }
        catch
        {
            return null;
        }
    }

    public RealGridClient(IConfiguration config, ILogger<RealGridClient>? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger;
    }

    public async Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Trace("GetColumnDataAndRowCountAsync.Param", new { secondCacheId });
        var client = CreateProductionClient(secondCacheId);
        var taskObj = InvokeWithSecondCacheIdAuto(client, "GetColumnDataAndRowCountAsync", secondCacheId);
        var raw = await AwaitTaskAsObject(taskObj).ConfigureAwait(false);
        var mapped = GridModelMapper.FromRawColumnDataAndRowCount(raw);
        Trace("GetColumnDataAndRowCountAsync.Result", Simplify(mapped));
        return mapped;
    }

    public async Task<GridDataResult> GetGridDataAsync(
        string[] columnIds,
        string gridId,
        bool isGetVirtualColumnValue,
        int height,
        int width,
        int x,
        int y,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Log who called this method and which value was passed in.
        // (We have seen cases where the caller passes false unexpectedly.)
        Trace("GetGridDataAsync.Entry", new
        {
            gridId,
            isGetVirtualColumnValue,
            caller = new System.Diagnostics.StackTrace(1, false)
                .GetFrame(0)?.GetMethod()?.DeclaringType?.FullName + "." +
                new System.Diagnostics.StackTrace(1, false)
                    .GetFrame(0)?.GetMethod()?.Name
        });

        var client = CreateProductionClient(gridId);

        // Production DTO name differs by environment / DXC dll version.
        // Observed names: DXC.EngPF.Core.Grid.GetGridDataParam (1.6.0.0) / DXC.EngPF.Core.Grid.GridDataParam (older).
        var gridDataParamType = FindType("DXC.EngPF.Core.Grid.GetGridDataParam")
                                ?? FindType("DXC.EngPF.Core.Grid.GridDataParam")
                                ?? throw new InvalidOperationException(
                                    "Type not found: DXC.EngPF.Core.Grid.GetGridDataParam / DXC.EngPF.Core.Grid.GridDataParam");
        var param = Activator.CreateInstance(gridDataParamType)
                   ?? throw new InvalidOperationException("Failed to create GridDataParam.");

        var okGridId = TrySetProp(param, "GridId", gridId, out var rGridId);
        var okX = TrySetProp(param, "X", x, out var rX);
        var okY = TrySetProp(param, "Y", y, out var rY);
        var okW = TrySetProp(param, "Width", width, out var rW);
        var okH = TrySetProp(param, "Height", height, out var rH);

        // Some DXC dlls expose different names and/or non-public setters.
        var okVirt = TrySetProp(param, "IsGetVirtualColumnValue", isGetVirtualColumnValue, out var rVirt);
        if (!okVirt) okVirt = TrySetProp(param, "isGetVirtualColumnValue", isGetVirtualColumnValue, out rVirt);
        if (!okVirt) okVirt = TrySetProp(param, "GetVirtualColumnValue", isGetVirtualColumnValue, out rVirt);

        // Display column list name differs in some environments.
        var colList = (columnIds ?? Array.Empty<string>()).ToList();
        var okCols = TrySetProp(param, "DisplayColumn", colList, out var rCols);
        if (!okCols) okCols = TrySetProp(param, "ColumnIds", colList, out rCols);
        if (!okCols) okCols = TrySetProp(param, "columnIds", colList, out rCols);

        Trace("GetGridDataAsync.SetProp", new
        {
            ParamType = param.GetType().FullName,
            isGetVirtualColumnValue,
            okVirt,
            rVirt,
            okCols,
            rCols,
            okGridId,
            rGridId,
            okX,
            rX,
            okY,
            rY,
            okW,
            rW,
            okH,
            rH
        });

        Trace("GetGridDataAsync.Param", Simplify(param));

        var taskObj = Invoke(client, "GetGridDataAsync", new object?[] { param });
        var raw = await AwaitTaskAsObject(taskObj).ConfigureAwait(false);
        var mapped = GridModelMapper.FromRawGridData(raw);
        Trace("GetGridDataAsync.Result", new
        {
            GridData = Simplify(mapped.GridData),
            ColumnIndexArray = Simplify(mapped.ColumnIndexArray)
        });
        return mapped;
    }

    public async Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var client = CreateProductionClient(secondCacheId);
        var taskObj = InvokeWithSecondCacheIdAuto(client, "GetGridFilterInfoAsync", secondCacheId);
        var raw = await AwaitTaskAsObject(taskObj).ConfigureAwait(false);
        return GridModelMapper.FromRawGridFilterInfo(raw);
    }

    public async Task<string> SetFilterAndSortAsync(FilterAndSortParam param, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var client = CreateProductionClient(param?.SecondCacheId ?? string.Empty);

        // Production DTO name differs by environment; in the DXC dll it is typically SetFilterAndSortParam.
        var tParam = FindType("DXC.EngPF.Core.Grid.SetFilterAndSortParam")
                     ?? FindType("DXC.EngPF.Core.Grid.FilterAndSortParam")
                     ?? throw new InvalidOperationException("SetFilterAndSort parameter type was not found.");

        var prodParam = Activator.CreateInstance(tParam)
                       ?? throw new InvalidOperationException("Failed to create SetFilterAndSortParam.");

        // Copy common properties.
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

    private object CreateProductionClient(string secondCacheId)
    {
        // Production client: DXC.EngPF.Core.Grid.Services.GridServiceClient(url, secondCacheId, TimeoutSec)
        var url = BuildUrl();

        // SessionId is stored by RequestId=7 (and/or RequestId=10) flow.
        // Prefer unified keys; keep compatibility with older keys.
        var sessionId =
            GlobalCache.Cache.Get<string>("RequestId7to10.SessionId")
            ?? GlobalCache.Cache.Get<string>("RequestId7.Parameter3")
            ?? GlobalCache.Cache.Get<string>("RequestId7:parameter3")
            ?? GlobalCache.Cache.Get<string>("Request7.Parameter3")
            ?? GlobalCache.Cache.Get<string>("Request7:parameter3");

        var clientType = FindType("DXC.EngPF.Core.Grid.Services.GridServiceClient")
                         ?? FindType("DXC.EngPF.Core.Grid.GridServiceClient")
                         ?? FindTypeByName("GridServiceClient");

        if (clientType is null)
        {
            throw new InvalidOperationException(
                "Production client type 'GridServiceClient' was not found. " +
                "Ensure DXC.EngPF.Core.Grid.dll is referenced and copied to output.");
        }

        try
        {
            object? client = null;
            var ctorUsed = "(none)";

            static bool NameContains(ParameterInfo p, params string[] parts)
            {
                var n = (p.Name ?? string.Empty).ToLowerInvariant();
                return parts.Any(x => n.Contains(x));
            }

            object? TryCreate3(string id, Func<ParameterInfo, bool> secondParamPred, string label)
            {
                foreach (var ctor in clientType.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
                {
                    var ps = ctor.GetParameters();
                    if (ps.Length != 3) continue;
                    if (ps[0].ParameterType != typeof(string)) continue;
                    if (ps[1].ParameterType != typeof(string)) continue;

                    // Timeout type may differ (int/long/TimeSpan) depending on DXC version.
                    try
                    {
                        if (!secondParamPred(ps[1])) continue;

                        object? timeoutArg = TimeoutSec;
                        if (ps[2].ParameterType == typeof(long)) timeoutArg = (long)TimeoutSec;
                        else if (ps[2].ParameterType == typeof(TimeSpan)) timeoutArg = TimeSpan.FromSeconds(TimeoutSec);

                        return ctor.Invoke(new object?[] { url, id, timeoutArg });
                    }
                    catch
                    {
                        // ignore and try next ctor
                    }
                }
                return null;
            }

            // 1) Prefer sessionId when available AND ctor second param name looks like session.
            if (!string.IsNullOrWhiteSpace(sessionId))
            {
                client = TryCreate3(sessionId, p => NameContains(p, "session"), "(url,sessionId,timeout)");
                if (client is not null) ctorUsed = "(url,sessionId,timeout)";
            }

            // 2) Then try secondCacheId ONLY if ctor second param name looks like second-cache.
            if (client is null)
            {
                client = TryCreate3(secondCacheId, p => NameContains(p, "second", "cache"), "(url,secondCacheId,timeout)");
                if (client is not null) ctorUsed = "(url,secondCacheId,timeout)";
            }

            // 3) Fallbacks: (url, timeout) or (url)
            if (client is null)
            {
                try
                {
                    client = Activator.CreateInstance(clientType, url, TimeoutSec);
                    if (client is not null) ctorUsed = "(url,timeout)";
                }
                catch { /* ignore */ }
            }

            if (client is null)
            {
                try
                {
                    client = Activator.CreateInstance(clientType, url);
                    if (client is not null) ctorUsed = "(url)";
                }
                catch { /* ignore */ }
            }

            if (client is null)
                throw new InvalidOperationException("Failed to construct GridServiceClient.");

            Trace("GridClient.Create", new { url, sessionId, secondCacheId, timeoutSec = TimeoutSec, ctorUsed });
            return client;
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
            throw; // unreachable
        }
    }

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

        var built = $"{baseUrl}/EngPF_t/services/grid/1_6_0_0/";
        _logger?.LogInformation("[GridApi] BuiltUrl={BuiltUrl}", built);
        return built;
    }

    private static string NormalizeBaseUrl(string baseUrl)
    {
        baseUrl = (baseUrl ?? string.Empty).Trim();

        if (!baseUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
            !baseUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
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
    // Reflection helpers
    // -----------------

    private static Type GetTypeOrThrow(string fullName)
        => FindType(fullName)
           ?? throw new InvalidOperationException($"Type not found: {fullName}");

    private static Type? FindType(string fullName)
    {
        // Look through already loaded assemblies first.
        foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var t = a.GetType(fullName, throwOnError: false, ignoreCase: false);
                if (t is not null) return t;
            }
            catch { /* ignore */ }
        }

        // As a fallback, try to load the production DLL from well-known locations.
        // This avoids a compile-time reference to DXC.EngPF.Core.Grid.dll.
        try
        {
            foreach (var p in EnumerateProductionGridDllCandidates())
            {
                if (!File.Exists(p)) continue;
                var asm = Assembly.LoadFrom(p);
                var t = asm.GetType(fullName, throwOnError: false, ignoreCase: false);
                if (t is not null) return t;
            }
        }
        catch { /* ignore */ }

        return null;
    }

    private static IEnumerable<string> EnumerateProductionGridDllCandidates()
    {
        var exeDir = AppContext.BaseDirectory;
        var libDir = Path.GetDirectoryName(typeof(RealGridClient).Assembly.Location) ?? exeDir;

        // IMPORTANT:
        //  - output 直下に "DXC.EngPF.Core.Grid.dll" が存在すると (例: DummyDLL が出力してしまうケース)
        //    誤ってそちらを LoadFrom してしまい、GridServiceClient が見つからない。
        //  - そのため ".../DLL" 配下を優先して探索する。

        // 1) exeDir/DLL, libDir/DLL (ローカル同梱)
        yield return Path.Combine(exeDir, "DLL", "DXC.EngPF.Core.Grid.dll");
        yield return Path.Combine(libDir, "DLL", "DXC.EngPF.Core.Grid.dll");

        // 2) solutionRoot/DLL を上方向に探索 (root/DLL を想定)
        foreach (var root in WalkUp(exeDir, 12))
            yield return Path.Combine(root, "DLL", "DXC.EngPF.Core.Grid.dll");
        foreach (var root in WalkUp(libDir, 12))
            yield return Path.Combine(root, "DLL", "DXC.EngPF.Core.Grid.dll");

        // 3) 最後に exeDir 直下 / libDir 直下 (念のため)
        yield return Path.Combine(exeDir, "DXC.EngPF.Core.Grid.dll");
        yield return Path.Combine(libDir, "DXC.EngPF.Core.Grid.dll");
    }

    private static IEnumerable<string> WalkUp(string startDir, int maxDepth)
    {
        var cur = new DirectoryInfo(startDir);
        for (var i = 0; i < maxDepth && cur is not null; i++)
        {
            yield return cur.FullName;
            cur = cur.Parent;
        }
    }

    private static Type? FindTypeByName(string shortName)
    {
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var t = asm.GetTypes().FirstOrDefault(x => string.Equals(x.Name, shortName, StringComparison.Ordinal));
                if (t is not null) return t;
            }
            catch
            {
                // ignore reflection-load failures
            }
        }
        return null;
    }

    private static object Invoke(object target, string methodName, object?[]? args)
    {
        var t = target.GetType();
        var mi = t.GetMethod(methodName, BindingFlags.Public | BindingFlags.Instance);
        if (mi is null)
            throw new MissingMethodException(t.FullName, methodName);
        try
        {
            return mi.Invoke(target, args) ?? throw new InvalidOperationException($"{methodName} returned null.");
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
            throw;
        }
    }

    private static object InvokeWithSecondCacheIdAuto(object client, string methodName, string secondCacheId)
    {
        var t = client.GetType();
        var cands = t.GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal))
            .ToArray();

        // Prefer (string) overload.
        var strOver = cands.FirstOrDefault(m =>
        {
            var ps = m.GetParameters();
            return ps.Length == 1 && ps[0].ParameterType == typeof(string);
        });
        if (strOver is not null)
        {
            var pName = (strOver.GetParameters()[0].Name ?? string.Empty).ToLowerInvariant();

            // If the parameter looks like a session id, use cached SessionId instead of SecondCacheId.
            if (pName.Contains("session"))
            {
                var sessionId =
                    GlobalCache.Cache.Get<string>("RequestId7to10.SessionId")
                    ?? GlobalCache.Cache.Get<string>("RequestId7.Parameter3")
                    ?? GlobalCache.Cache.Get<string>("RequestId7:parameter3")
                    ?? GlobalCache.Cache.Get<string>("Request7.Parameter3")
                    ?? GlobalCache.Cache.Get<string>("Request7:parameter3");

                var arg = string.IsNullOrWhiteSpace(sessionId) ? secondCacheId : sessionId;
                return strOver.Invoke(client, new object?[] { arg })
                       ?? throw new InvalidOperationException($"{methodName} returned null.");
            }

            return strOver.Invoke(client, new object?[] { secondCacheId })
                   ?? throw new InvalidOperationException($"{methodName} returned null.");
        }

        // Otherwise, create request DTO and set SecondCacheId.
        var oneParam = cands.FirstOrDefault(m => m.GetParameters().Length == 1);
        if (oneParam is null)
            throw new MissingMethodException(t.FullName, methodName);

        var pType = oneParam.GetParameters()[0].ParameterType;
        var dto = Activator.CreateInstance(pType)
                  ?? throw new InvalidOperationException($"Failed to create param for {methodName}: {pType.FullName}");
        SetProp(dto, "SecondCacheId", secondCacheId);
        SetProp(dto, "PreSecondCacheId", secondCacheId);

        return oneParam.Invoke(client, new object?[] { dto })
               ?? throw new InvalidOperationException($"{methodName} returned null.");
    }

    private static void SetProp(object obj, string propName, object? value)
    {
        var p = obj.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
        if (p is null || !p.CanWrite) return;
        p.SetValue(obj, value);
    }

    /// <summary>
    /// Tries to set a property with diagnostics. Uses IgnoreCase and allows non-public setters.
    /// </summary>
    private static bool TrySetProp(object obj, string propName, object? value, out string reason)
    {
        reason = "OK";
        var t = obj.GetType();

        // IgnoreCase: DXC DTOs differ by casing in some environments.
        var p = t.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (p is null)
        {
            reason = $"Property not found: {propName}";
            return false;
        }

        // allow non-public setters
        var setMethod = p.GetSetMethod(true);
        if (setMethod is null)
        {
            reason = $"No setter: {p.Name}";
            return false;
        }

        try
        {
            object? v = value;
            if (v is not null && !p.PropertyType.IsInstanceOfType(v))
            {
                // Common conversions (bool/int/string)
                v = Convert.ChangeType(v, p.PropertyType);
            }

            setMethod.Invoke(obj, new object?[] { v });
            return true;
        }
        catch (Exception ex)
        {
            reason = $"Set failed: {p.Name} ({ex.GetType().Name}: {ex.Message})";
            return false;
        }
    }

    private static async Task<object> AwaitTaskAsObject(object taskObj)
    {
        if (taskObj is not Task t)
            throw new InvalidOperationException("Expected Task.");

        await t.ConfigureAwait(false);
        var tt = taskObj.GetType();
        // Task<T>.Result
        var resultProp = tt.GetProperty("Result", BindingFlags.Public | BindingFlags.Instance);
        if (resultProp is null)
            throw new InvalidOperationException("Task has no Result.");
        return resultProp.GetValue(taskObj) ?? throw new InvalidOperationException("Task.Result is null.");
    }

    private static async Task AwaitVoidTask(object taskObj)
    {
        if (taskObj is not Task t)
            throw new InvalidOperationException("Expected Task.");
        await t.ConfigureAwait(false);
    }
}