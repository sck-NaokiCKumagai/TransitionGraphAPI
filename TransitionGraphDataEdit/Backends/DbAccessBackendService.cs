using Grpc.Net.Client;
using System.Net.Http;
using System.Globalization;
using TransitionGraph.Shared;
using TransitionGraph.Dtos;
using TransitionGraph.V1;
using TransitionGraphDataEdit.Models;
using System.Text.Json;

namespace TransitionGraphDataEdit.Backends;

/// <summary>
/// Backend implementation using TrasitionGraphDBAccess (gRPC DBAccess service).
///
/// This is the default backend for RequestId 2-6.
/// RequestId=1 is handled by <see cref="LocalBackendService"/>.
/// </summary>
public sealed class DbAccessBackendService : IBackendService
{
    private readonly DataDir _dir;
    private readonly WorkerAddresses _addrs;

    public DbAccessBackendService(DataDir dir, WorkerAddresses addrs)
    {
        _dir = dir;
        _addrs = addrs;
    }

    public async Task<BuildReply> BuildAsync(BuildRequest request, string resultId, CancellationToken cancellationToken)
    {
        // NOTE:
        // - This backend writes raw Point rows to ResultStore.
        // - Later phases can replace it with a streaming/paging approach.

        var header = request.Header ?? new RequestHeader();

        var sqlKey = request.SqlKey ?? string.Empty;

        // ======================================================
        // RequestId=4: ロット情報取得 (LotInformationDto)
        // Spec:
        // - parameter[0] (Parm1): process suffix "1PC" or "2PC"
        // - parameter[1] (Parm2..): lot names list (WPF List<string>), passed as lotNamesJson or Parm2..
        // Data source:
        // - MEASURE_WAFER_HISTORY_(proc): LotName, WaferNo, WaferId, BaseProcess, Work
        // - MEASURE_LOT_HISTORY_(proc): DEVTYPE_INFO
        // Output:
        // - ResultStore: single row "1,<LotInformationDto as JSON>"
        // ======================================================
        if (string.Equals(header.RequestId, "4", StringComparison.OrdinalIgnoreCase))
        {
            // parameter mapping (REST -> gRPC): Parm1..Parm10
            request.Parameters.TryGetValue("Parm1", out var proc4);
            proc4 ??= "1PC";
            proc4 = proc4.Trim();
            if (!string.Equals(proc4, "1PC", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(proc4, "2PC", StringComparison.OrdinalIgnoreCase))
            {
                throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument,
                    "RequestId=4 requires parameter[0] (Parm1) = '1PC' or '2PC'"));
            }


            // lot names: prefer lotNamesJson (WPF list), fallback to Parm2..Parm10 (each lot name), lastly fallback to legacy CSV in Parm2
            var lotNames = new List<string>();
            if (request.Parameters.TryGetValue("lotNamesJson", out var lotJson) && !string.IsNullOrWhiteSpace(lotJson))
            {
                try
                {
                    lotNames = JsonSerializer.Deserialize<List<string>>(lotJson) ?? new List<string>();
                }
                catch
                {
                    // ignore and fallback
                }
            }

            if (lotNames.Count == 0)
            {
                for (var p = 2; p <= 10; p++)
                {
                    if (request.Parameters.TryGetValue($"Parm{p}", out var v) && !string.IsNullOrWhiteSpace(v))
                        lotNames.Add(v.Trim());
                }
            }

            if (lotNames.Count == 0)
            {
                request.Parameters.TryGetValue("Parm2", out var lotRaw);
                lotRaw ??= "";
                lotNames = SplitToList(lotRaw);
            }

            if (lotNames.Count == 0)
            {
                throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument,
                    "RequestId=4 requires parameter[1] as lot name list"));
            }

            var lotNamesCsv = NormalizeStringListToCsv(lotNames);

            // Query 1) Wafer history
            var waferSqlKey = string.Equals(proc4, "2PC", StringComparison.OrdinalIgnoreCase)
                ? "MEASURE_WAFER_HISTORY_2PC"
                : "MEASURE_WAFER_HISTORY_1PC";

            // Query 2) Lot history (DEVTYPE_INFO)
            var devtypeSqlKey = string.Equals(proc4, "2PC", StringComparison.OrdinalIgnoreCase)
                ? "MEASURE_LOT_DEVTYPE_2PC"
                : "MEASURE_LOT_DEVTYPE_1PC";

            using var ch4 = GrpcChannel.ForAddress(_addrs.DbAccessAddress, GrpcChannelFactory.Options());
            var client4 = new DBAccess.DBAccessClient(ch4);

            var waferRid = Guid.NewGuid().ToString();
            var devRid = Guid.NewGuid().ToString();

            await client4.OpenQueryAsync(new OpenQueryRequest
            {
                ResultId = waferRid,
                SqlKey = waferSqlKey,
                Parameters = { { "lotNamesCsv", lotNamesCsv } }
            }, cancellationToken: cancellationToken);

            await client4.OpenQueryAsync(new OpenQueryRequest
            {
                ResultId = devRid,
                SqlKey = devtypeSqlKey,
                Parameters = { { "lotNamesCsv", lotNamesCsv } }
            }, cancellationToken: cancellationToken);

            var waferValues = await FetchAllValuesAsync(client4, waferRid, cancellationToken);
            var devValues = await FetchAllValuesAsync(client4, devRid, cancellationToken);

            // DEVTYPE_INFO map (lot -> productInfo)
            var devMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in devValues)
            {
                var parts = line.Split('\t');
                if (parts.Length < 2) continue;
                var lot = parts[0].Trim();
                var dev = parts[1].Trim();
                if (string.IsNullOrWhiteSpace(lot)) continue;
                if (!devMap.ContainsKey(lot)) devMap[lot] = dev;
            }

            var list = new List<LotInfoDto>(capacity: Math.Max(16, waferValues.Count));
            foreach (var line in waferValues)
            {
                var parts = line.Split('\t');
                // LotName, WaferNo, WaferId, BaseProcess, Work
                if (parts.Length < 5) continue;
                var lot = parts[0].Trim();
                if (string.IsNullOrWhiteSpace(lot)) continue;

                int waferNo = 0;
                _ = int.TryParse(parts[1].Trim(), out waferNo);

                var waferId = parts[2].Trim();
                var baseProc = parts[3].Trim();
                var work = parts[4].Trim();

                devMap.TryGetValue(lot, out var dev);
                dev ??= string.Empty;

                list.Add(new LotInfoDto
                {
                    LotName = lot,
                    ProductInfo = dev,
                    WaferNo = waferNo,
                    WaferId = waferId,
                    BaseProcess = baseProc,
                    Work = work
                });
            }

            var lotInformation = new LotInformationDto { LotInfo = list };
            var json = System.Text.Json.JsonSerializer.Serialize(lotInformation);
            ResultStore.WriteAll(_dir.Path, resultId, new List<(int RowNo, string Value)> { (1, json) });

            return new BuildReply
            {
                Header = header,
                ResultId = resultId,
                TotalCount = 1,
                Message = $"Ready (LotInformationDto LotInfo={list.Count})"
            };
        }

        // ======================================================
        // RequestId=5: build GraphDataDto JSON (Spec)
        // ======================================================
        if (string.Equals(header.RequestId, "5", StringComparison.OrdinalIgnoreCase))
        {
            // parameter mapping (REST -> gRPC): Parm1..Parm10
            // Spec: parameter[3] indicates data grain ("Lot" / "Wafer")
            request.Parameters.TryGetValue("Parm4", out var dataSort);
            dataSort ??= "Lot";

            sqlKey = string.Equals(dataSort, "Wafer", StringComparison.OrdinalIgnoreCase)
                ? "WAFER_STATISTIC_DATA"
                : "LOT_STATISTIC_DATA";

            // Build list parameters:
            // 1) Prefer Header.RequestKey (WPF-style lot->tests dictionary)
            // 2) Fallback to Parm2/Parm3 (batch/client sends JSON array string or CSV)
            var lotNames = header.RequestKey.Keys.ToList();
            var selectedItemName = header.RequestKey.Values
                .SelectMany(v => v.ItemName)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault() ?? string.Empty;

            if (lotNames.Count == 0)
            {
                request.Parameters.TryGetValue("Parm2", out var lotRaw);
                lotRaw ??= "";
                lotNames = ParseStringListFlexible(lotRaw);
            }

            if (string.IsNullOrWhiteSpace(selectedItemName))
            {
                request.Parameters.TryGetValue("Parm3", out var testRaw);
                testRaw ??= "";
                selectedItemName = ParseSingleStringFlexible(testRaw);
            }

            // Keep internal APIs that expect list<string> by wrapping the single value.
            var testNames = string.IsNullOrWhiteSpace(selectedItemName)
                ? new List<string>()
                : new List<string> { selectedItemName };

            request.Parameters["lotNamesCsv"] = NormalizeStringListToCsv(lotNames);
            request.Parameters["testNamesCsv"] = selectedItemName;

            // Representative lot (optional) - Parm5 per spec
            request.Parameters.TryGetValue("Parm5", out var mainLot);
            mainLot ??= "";

            // Process suffix (optional) - Parm6 per spec (used for HistoryInfo)
            request.Parameters.TryGetValue("Parm6", out var proc5);
            proc5 ??= "";
            proc5 = proc5.Trim();

            using var ch5 = GrpcChannel.ForAddress(_addrs.DbAccessAddress, GrpcChannelFactory.Options());
            var client5 = new DBAccess.DBAccessClient(ch5);

            // Execute main query (LOT_STATISTIC_DATA or WAFER_STATISTIC_DATA)
            await client5.OpenQueryAsync(new OpenQueryRequest
            {
                ResultId = resultId,
                SqlKey = sqlKey,
                Parameters = { request.Parameters }
            }, cancellationToken: cancellationToken);

            const int fetchChunkSize5 = 5000;
            var all5 = new List<(int RowNo, string Value)>(capacity: 1024);
            int lastRowNo5 = 0;
            while (true)
            {
                var rep = await client5.FetchPageAsync(new FetchPageRequest
                {
                    ResultId = resultId,
                    LastRowNo = lastRowNo5,
                    PageSize = fetchChunkSize5
                }, cancellationToken: cancellationToken);

                if (rep.Items is not null && rep.Items.Count > 0)
                {
                    foreach (var p in rep.Items)
                        all5.Add((p.RowNo, p.Value));
                }

                if (rep.Done || rep.ReturnedCount <= 0 || rep.NextLastRowNo <= 0)
                    break;

                lastRowNo5 = rep.NextLastRowNo;
            }

            // Load SPEC/LIMIT for representative lot per TEST_NAME (best-effort)
            var specMap = await LoadSpecLimitMapAsync(client5, mainLot, testNames, cancellationToken);

            // Load HistoryInfo (EQUIPMENT_ID / PROGRAM) per lot (best-effort)
            var historyMap = await LoadLotHistoryInfoMapAsync(client5, proc5, lotNames, cancellationToken);

            var graphDtos = BuildGraphDataDto(
                rows: all5.Select(t => t.Value).ToList(),
                dataSort: dataSort,
                expectedLotNames: lotNames,
                expectedTestNames: testNames,
                specMap: specMap,
                historyMap: historyMap
            );

            // Store as JSON per row (1 row per ItemName/TEST_NAME)
            var jsonLines = graphDtos
                .Select((g, idx) => (idx + 1, System.Text.Json.JsonSerializer.Serialize(g)))
                .ToList();

            ResultStore.WriteAll(_dir.Path, resultId, jsonLines);

            return new BuildReply
            {
                Header = header,
                ResultId = resultId,
                TotalCount = jsonLines.Count,
                Message = $"Ready (GraphDataDto {jsonLines.Count} rows)"
            };
        }

        // ======================================================
        // RequestId=6: HistGraphDataDto (GraphDataDto + Chips)
        // ======================================================
        if (string.Equals(header.RequestId, "6", StringComparison.OrdinalIgnoreCase))
        {
            // parameter mapping (REST -> gRPC): Parm1..Parm10
            // Spec: parameter[4] indicates x-axis / grain ("Lot" / "Wafer")
            request.Parameters.TryGetValue("Parm4", out var dataSort6);
            dataSort6 ??= "Lot";
            dataSort6 = dataSort6.Trim();

            // Build list parameters:
            // 1) Prefer Header.RequestKey (WPF-style lot->tests dictionary)
            // 2) Fallback to Parm2/Parm3 (batch/client sends JSON array string or CSV)
            var lotNames6 = header.RequestKey.Keys.ToList();
            var selectedItemName6 = header.RequestKey.Values
                .SelectMany(v => v.ItemName)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault() ?? string.Empty;

            if (lotNames6.Count == 0)
            {
                request.Parameters.TryGetValue("Parm2", out var lotRaw6);
                lotRaw6 ??= "";
                lotNames6 = ParseStringListFlexible(lotRaw6);
            }

            if (string.IsNullOrWhiteSpace(selectedItemName6))
            {
                request.Parameters.TryGetValue("Parm3", out var testRaw6);
                testRaw6 ??= "";
                selectedItemName6 = ParseSingleStringFlexible(testRaw6);
            }

            // Keep internal APIs that expect list<string> by wrapping the single value.
            var testNames6 = string.IsNullOrWhiteSpace(selectedItemName6)
                ? new List<string>()
                : new List<string> { selectedItemName6 };

            request.Parameters["lotNamesCsv"] = NormalizeStringListToCsv(lotNames6);
            request.Parameters["testNamesCsv"] = selectedItemName6;

            // Representative lot (optional) - Parm5 per spec
            request.Parameters.TryGetValue("Parm5", out var mainLot6);
            mainLot6 ??= "";

            // Process suffix / 工程名 - Parm6 per spec (1PC / 2PC)
            request.Parameters.TryGetValue("Parm6", out var proc6);
            proc6 ??= "";
            proc6 = proc6.Trim();

            using var ch6 = GrpcChannel.ForAddress(_addrs.DbAccessAddress, GrpcChannelFactory.Options());
            var client6 = new DBAccess.DBAccessClient(ch6);

            // Load SPEC/LIMIT for representative lot per TEST_NAME (best-effort)
            var specMap6 = await LoadSpecLimitMapAsync(client6, mainLot6, testNames6, cancellationToken);

            // Load HistoryInfo (EQUIPMENT_ID / PROGRAM) per lot (best-effort)
            var historyMap6 = await LoadLotHistoryInfoMapAsync(client6, proc6, lotNames6, cancellationToken);

            // CSV params for DBAccess queries (explicit locals)
            var lotCsv6 = NormalizeStringListToCsv(lotNames6);
            var testCsv6 = NormalizeStringListToCsv(testNames6);

            // Execute WAFER_ID_MAP (WAFER_NO -> WAFER_ID)
            var waferMapRid = Guid.NewGuid().ToString("N");
            await client6.OpenQueryAsync(new OpenQueryRequest
            {
                ResultId = waferMapRid,
                SqlKey = "WAFER_ID_MAP",
                Parameters = { { "lotNamesCsv", lotCsv6 }, { "testNamesCsv", testCsv6 } }
            }, cancellationToken: cancellationToken);
            var waferMapRows = await FetchAllValuesAsync(client6, waferMapRid, cancellationToken);

            // Execute CHIP_MEASURE_DATA_(1PC/2PC)
            string chipSqlKey = proc6.Equals("2PC", StringComparison.OrdinalIgnoreCase)
                ? "CHIP_MEASURE_DATA_2PC"
                : "CHIP_MEASURE_DATA_1PC";

            var chipRid = Guid.NewGuid().ToString("N");
            await client6.OpenQueryAsync(new OpenQueryRequest
            {
                ResultId = chipRid,
                SqlKey = chipSqlKey,
                Parameters = { { "lotNamesCsv", lotCsv6 }, { "testNamesCsv", testCsv6 } }
            }, cancellationToken: cancellationToken);
            var chipRows = await FetchAllValuesAsync(client6, chipRid, cancellationToken);

            var graphDtos6 = BuildHistGraphDataDto6(
                waferIdMapRows: waferMapRows,
                chipMeasureRows: chipRows,
                expectedLotNames: lotNames6,
                expectedTestNames: testNames6,
                specMap: specMap6,
                historyMap: historyMap6
            );

            var jsonLines6 = graphDtos6
                .Select((g, idx) => (idx + 1, System.Text.Json.JsonSerializer.Serialize(g)))
                .ToList();

            ResultStore.WriteAll(_dir.Path, resultId, jsonLines6);

            return new BuildReply
            {
                Header = header,
                ResultId = resultId,
                TotalCount = jsonLines6.Count,
                Message = $"Ready (HistGraphDataDto {jsonLines6.Count} rows)"
            };
        }

        // NOTE: For all other RequestIds, run the default DBAccess flow.
        // Wrap in an explicit block scope to avoid any local variable name collisions
        // even if the compiler version treats earlier declarations differently.
        {
            // RequestId=2 : choose SQLKey by process parameter (compat)
            if (string.Equals(header.RequestId, "2", StringComparison.OrdinalIgnoreCase))
            {
                if (!request.Parameters.ContainsKey("startTime") || !request.Parameters.ContainsKey("endTime"))
                    throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument,
                        "RequestId=2 requires parameters.startTime and parameters.endTime"));

                request.Parameters.TryGetValue("process", out var proc);
                proc ??= "";
                if (string.Equals(proc, "1PC", StringComparison.OrdinalIgnoreCase))
                    sqlKey = "LOT_TEST_NAMES_1PC";
                else if (string.Equals(proc, "2PC", StringComparison.OrdinalIgnoreCase))
                    sqlKey = "LOT_TEST_NAMES_2PC";
                else
                    throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument,
                        "RequestId=2 requires parameters.process = '1PC' or '2PC'"));
            }

            using var ch = GrpcChannel.ForAddress(_addrs.DbAccessAddress, GrpcChannelFactory.Options());
            var client = new DBAccess.DBAccessClient(ch);

            var open = new OpenQueryRequest
            {
                ResultId = resultId,
                SqlKey = sqlKey
            };
            // IMPORTANT (RequestId=2):
            // - WPF/API spec includes "process" in parameters[], and DataEdit uses it only to choose SqlKey.
            // - DBAccess layer validates allowedParameters from SqlCatalog.json. For LOT_TEST_NAMES_1PC/2PC,
            //   allowed parameters are only startTime/endTime, so passing "process" causes InvalidArgument and the caller sees NG.
            // Therefore, when RequestId=2, pass only startTime/endTime to DBAccess, while keeping "process" in DataEdit.
            if (string.Equals(header.RequestId, "2", StringComparison.OrdinalIgnoreCase))
            {
                //if (request.Parameters.TryGetValue("startTime", out var st) && st is not null)
                //    open.Parameters["startTime"] = st;
                //if (request.Parameters.TryGetValue("endTime", out var et) && et is not null)
                //    open.Parameters["endTime"] = et;

                // DBAccess 側の allowedParameters に合わせて必要最小限のみ渡す
                var send = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                if (request.Parameters.TryGetValue("startTime", out var st) && !string.IsNullOrWhiteSpace(st))
                    send["startTime"] = st;

                if (request.Parameters.TryGetValue("endTime", out var et) && !string.IsNullOrWhiteSpace(et))
                    send["endTime"] = et;

                // ここでは process/Parm* は渡さない（process は既に sqlKey 選択に使用済み）
                open.Parameters.Add(send);
            }
            else
            {
                open.Parameters.Add(request.Parameters);
            }

            await client.OpenQueryAsync(open, cancellationToken: cancellationToken);

            const int fetchChunkSize = 5000;
            var all = new List<(int RowNo, string Value)>(capacity: 1024);
            int lastRowNo = 0;

            while (true)
            {
                var rep = await client.FetchPageAsync(new FetchPageRequest
                {
                    ResultId = resultId,
                    LastRowNo = lastRowNo,
                    PageSize = fetchChunkSize
                }, cancellationToken: cancellationToken);

                if (rep.Items is not null && rep.Items.Count > 0)
                {
                    foreach (var p in rep.Items)
                        all.Add((p.RowNo, p.Value));
                }

                if (rep.Done || rep.ReturnedCount <= 0 || rep.NextLastRowNo <= 0)
                    break;

                lastRowNo = rep.NextLastRowNo;
            }

            // RequestId=2 : LOT_NAME -> TEST_NAME dictionary JSON stored as one record
            if (string.Equals(header.RequestId, "2", StringComparison.OrdinalIgnoreCase))
            {
                var dict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var (_, value) in all)
                {
                    if (string.IsNullOrEmpty(value))
                        continue;

                    var parts = value.Split('\t', 2);
                    if (parts.Length != 2)
                        continue;

                    var lot = parts[0];
                    var test = parts[1];

                    if (!dict.TryGetValue(lot, out var list))
                    {
                        list = new List<string>();
                        dict[lot] = list;
                    }

                    if (!list.Contains(test, StringComparer.OrdinalIgnoreCase))
                        list.Add(test);
                }

                var json = System.Text.Json.JsonSerializer.Serialize(dict);
                ResultStore.WriteAll(_dir.Path, resultId, new[] { (1, json) });

                return new BuildReply
                {
                    Header = header,
                    ResultId = resultId,
                    TotalCount = 1,
                    Message = "Ready (dict json)"
                };
            }

            ResultStore.WriteAll(_dir.Path, resultId, all);

            return new BuildReply
            {
                Header = header,
                ResultId = resultId,
                TotalCount = all.Count,
                Message = $"Ready ({all.Count} rows)"
            };
        }
    }

    private static string NormalizeStringListToCsv(IEnumerable<string> src)
    {
        var list = src
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        return string.Join(",", list);
    }

    private static List<string> SplitToList(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return new List<string>();

        // Accept: "A,B,C" or "A;B;C" or "A B C" (screen/app may vary)
        var seps = new[] { ',', ';', '\t', '\r', '\n' };
        var parts = raw
            .Split(seps, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();
        return parts;
    }

    /// <summary>
    /// Flexible parser for list parameters coming from various clients.
    /// Accepts JSON array (e.g. ["A","B"]), JSON string (e.g. "A"), or CSV-like strings (e.g. A,B).
    /// </summary>
    private static List<string> ParseStringListFlexible(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return new List<string>();

        raw = raw.Trim();

        // Try JSON first (array or string)
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                var list = new List<string>();
                foreach (var e in doc.RootElement.EnumerateArray())
                {
                    if (e.ValueKind == System.Text.Json.JsonValueKind.String)
                        list.Add(e.GetString() ?? string.Empty);
                    else
                        list.Add(e.ToString());
                }
                return list.Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s.Trim()).ToList();
            }
            if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.String)
            {
                var s = doc.RootElement.GetString() ?? string.Empty;
                // If the string itself is CSV-like, split it.
                var split = SplitToList(s);
                return split.Count > 0 ? split : new List<string> { s };
            }
        }
        catch
        {
            // Not JSON -> fall back to CSV splitting
        }

        return SplitToList(raw);
    }

    private static async Task<List<string>> FetchAllValuesAsync(DBAccess.DBAccessClient client, string rid, CancellationToken ct)
    {
        const int fetchChunkSize = 5000;
        var all = new List<string>(capacity: 1024);
        int lastRowNo = 0;
        while (true)
        {
            var rep = await client.FetchPageAsync(new FetchPageRequest
            {
                ResultId = rid,
                LastRowNo = lastRowNo,
                PageSize = fetchChunkSize
            }, cancellationToken: ct);

            if (rep.Items is not null && rep.Items.Count > 0)
            {
                foreach (var p in rep.Items)
                    all.Add(p.Value ?? string.Empty);
            }

            if (rep.Done || rep.ReturnedCount <= 0 || rep.NextLastRowNo <= 0)
                break;

            lastRowNo = rep.NextLastRowNo;
        }

        return all;
    }

    private static List<GraphData5Dto> BuildGraphData5(
        List<string> rows,
        string dataSort,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap,
        List<string> expectedLotNames,
        List<string> expectedItemNames)
    {
        // Rows format (tab-separated):
        //  Lot:   LOT_NAME, TEST_NAME, MIN, MAX, AVG, MEDIAN, COUNT, STD, IQR, HISTORY(optional)
        //  Wafer: LOT_NAME, TEST_NAME, WAFER_NO, MIN, MAX, AVG, MEDIAN, COUNT, STD, IQR, HISTORY(optional)

        var isWafer = string.Equals(dataSort, "Wafer", StringComparison.OrdinalIgnoreCase);
        var parsed = new List<ParsedStatRow>(capacity: rows.Count);

        foreach (var r in rows)
        {
            if (string.IsNullOrWhiteSpace(r)) continue;
            var parts = r.Split('\t');
            if ((!isWafer && parts.Length < 9) || (isWafer && parts.Length < 10))
                continue;

            try
            {
                if (!isWafer)
                {
                    parsed.Add(new ParsedStatRow(
                        Lot: parts[0],
                        Item: parts[1],
                        WaferNo: null,
                        Min: ToDouble(parts[2]),
                        Max: ToDouble(parts[3]),
                        Avg: ToDouble(parts[4]),
                        Med: ToDouble(parts[5]),
                        Cnt: ToInt(parts[6]),
                        Std: ToDouble(parts[7]),
                        Iqr: ToDouble(parts[8]),
                        History: parts.Length >= 10 ? parts[9] : ""));
                }
                else
                {
                    parsed.Add(new ParsedStatRow(
                        Lot: parts[0],
                        Item: parts[1],
                        WaferNo: int.TryParse(parts[2], out var w) ? w : (int?)null,
                        Min: ToDouble(parts[3]),
                        Max: ToDouble(parts[4]),
                        Avg: ToDouble(parts[5]),
                        Med: ToDouble(parts[6]),
                        Cnt: ToInt(parts[7]),
                        Std: ToDouble(parts[8]),
                        Iqr: ToDouble(parts[9]),
                        History: parts.Length >= 11 ? parts[10] : ""));
                }
            }
            catch
            {
                // skip malformed row
            }
        }

        var result = new List<GraphData5Dto>();

        // Build quick lookup for returned rows
        var groupMap = parsed
            .GroupBy(p => (p.Lot, p.Item), StringTupleComparer.Instance)
            .ToDictionary(g => g.Key, g => g.ToList(), StringTupleComparer.Instance);

        foreach (var lot in expectedLotNames)
        {
            foreach (var item in expectedItemNames)
            {
                var dto = new GraphData5Dto
                {
                    LotName = lot,
                    ItemName = item,
                    DataSort = dataSort
                };

                if (!groupMap.TryGetValue((lot, item), out var rowsForKey))
                {
                    // Missing data: set point data to null per spec
                    if (isWafer)
                        dto.WaferGraphPointData = new Dictionary<string, GraphPointData5Dto?>();

                    dto.StatisticsInfo = new StatisticsInfoDto();
                    result.Add(dto);
                    continue;
                }

                if (!isWafer)
                {
                    // One point per lot
                    var p = rowsForKey.First();
                    dto.GraphPointData = new GraphPointData5Dto
                    {
                        Minimum = p.Min,
                        Maximum = p.Max,
                        Average = p.Avg,
                        Median = p.Med,
                        DataCount = p.Cnt,
                        Sigma = p.Std,
                        IQR = p.Iqr,
                        HistoryInfo = ParseHistoryInfo(p.History)
                    };
                    // When dataSort is "Lot", wafer dictionary must be null (default)
                    dto.StatisticsInfo = BuildStatisticsInfo(new[] { dto.GraphPointData }, item, specMap);
                }
                else
                {
                    // Wafer points dictionary with skip filling
                    dto.WaferGraphPointData = new Dictionary<string, GraphPointData5Dto?>();

                    var map = new Dictionary<int, GraphPointData5Dto>();
                    foreach (var p in rowsForKey)
                    {
                        if (p.WaferNo is null || p.WaferNo <= 0) continue;
                        map[p.WaferNo.Value] = new GraphPointData5Dto
                        {
                            Minimum = p.Min,
                            Maximum = p.Max,
                            Average = p.Avg,
                            Median = p.Med,
                            DataCount = p.Cnt,
                            Sigma = p.Std,
                            IQR = p.Iqr,
                            HistoryInfo = ParseHistoryInfo(p.History)
                        };
                    }

                    var maxNo = map.Keys.Count == 0 ? 0 : map.Keys.Max();
                    for (var i = 1; i <= maxNo; i++)
                    {
                        dto.WaferGraphPointData[i.ToString()] = map.TryGetValue(i, out var v) ? v : null;
                    }

                    // When dataSort is "Wafer", lot point must be null per spec
                    dto.GraphPointData = null;

                    var nonNull = dto.WaferGraphPointData.Values
                        .Where(v => v is not null)
                        .Cast<GraphPointData5Dto>()
                        .ToList();

                    dto.StatisticsInfo = BuildStatisticsInfo(nonNull, item, specMap);
                }

                result.Add(dto);
            }
        }

        return result;
    }

    private static List<GraphDataDto> BuildGraphDataDto(
        List<string> rows,
        string dataSort,
        List<string> expectedLotNames,
        List<string> expectedTestNames,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap,
        Dictionary<string, List<string>> historyMap)
    {
        // Rows format (tab-separated):
        //  Lot:   LOT_NAME, TEST_NAME, MIN, MAX, AVG, MEDIAN, COUNT, STD, IQR, HISTORY(optional)
        //  Wafer: LOT_NAME, TEST_NAME, WAFER_ID(or WAFER_NO), MIN, MAX, AVG, MEDIAN, COUNT, STD, IQR, HISTORY(optional)

        var isWafer = string.Equals(dataSort, "Wafer", StringComparison.OrdinalIgnoreCase);

        var parsed = new List<(string Lot, string Test, string? WaferId, double Min, double Max, double Avg, double Med, int Cnt, double Std, double Iqr, string History)>(rows.Count);

        foreach (var r in rows)
        {
            if (string.IsNullOrWhiteSpace(r)) continue;
            var parts = r.Split('\t');

            if (!isWafer)
            {
                if (parts.Length < 9) continue;
                parsed.Add((
                    Lot: parts[0],
                    Test: parts[1],
                    WaferId: null,
                    Min: ToDouble(parts[2]),
                    Max: ToDouble(parts[3]),
                    Avg: ToDouble(parts[4]),
                    Med: ToDouble(parts[5]),
                    Cnt: ToInt(parts[6]),
                    Std: ToDouble(parts[7]),
                    Iqr: ToDouble(parts[8]),
                    History: parts.Length >= 10 ? parts[9] : ""
                ));
            }
            else
            {
                if (parts.Length < 10) continue;
                parsed.Add((
                    Lot: parts[0],
                    Test: parts[1],
                    WaferId: parts[2],
                    Min: ToDouble(parts[3]),
                    Max: ToDouble(parts[4]),
                    Avg: ToDouble(parts[5]),
                    Med: ToDouble(parts[6]),
                    Cnt: ToInt(parts[7]),
                    Std: ToDouble(parts[8]),
                    Iqr: ToDouble(parts[9]),
                    History: parts.Length >= 11 ? parts[10] : ""
                ));
            }
        }

        // (lot, test) -> rows
        var groupMap = parsed
            .GroupBy(p => (p.Lot, p.Test), StringTupleComparer.Instance)
            .ToDictionary(g => g.Key, g => g.ToList(), StringTupleComparer.Instance);

        var results = new List<GraphDataDto>(capacity: Math.Max(1, expectedTestNames.Count));

        foreach (var test in expectedTestNames)
        {
            var dto = new GraphDataDto
            {
                ItemName = test
            };

            var allUnderlyingStats = new List<StatsDto>(capacity: Math.Max(16, expectedLotNames.Count));

            foreach (var lot in expectedLotNames)
            {
                var lotDto = new LotDto { LotName = lot };

                // Attach history (best-effort)
                if (historyMap.TryGetValue(lot, out var hist) && hist is not null)
                    lotDto.Stats.HistoryInfo = hist;

                if (!groupMap.TryGetValue((lot, test), out var rowsForKey) || rowsForKey.Count == 0)
                {
                    // Missing data -> keep zeros, wafers empty
                    dto.Lots.Add(lotDto);
                    if (!isWafer) allUnderlyingStats.Add(lotDto.Stats);
                    continue;
                }

                if (!isWafer)
                {
                    var p = rowsForKey[0];
                    lotDto.Stats = new StatsDto
                    {
                        Minimum = p.Min,
                        Maximum = p.Max,
                        Average = p.Avg,
                        Median = p.Med,
                        DataCount = p.Cnt,
                        Sigma = p.Std,
                        IQR = p.Iqr,
                        HistoryInfo = (historyMap.TryGetValue(lot, out var h) && h is not null) ? h : new List<string>()
                    };

                    dto.Lots.Add(lotDto);
                    allUnderlyingStats.Add(lotDto.Stats);
                }
                else
                {
                    // Wafer list
                    foreach (var p in rowsForKey)
                    {
                        var waferId = p.WaferId ?? string.Empty;
                        var w = new WaferDto
                        {
                            WaferId = waferId,
                            Stats = new StatsDto
                            {
                                Minimum = p.Min,
                                Maximum = p.Max,
                                Average = p.Avg,
                                Median = p.Med,
                                DataCount = p.Cnt,
                                Sigma = p.Std,
                                IQR = p.Iqr
                            }
                        };
                        lotDto.Wafers.Add(w);
                        allUnderlyingStats.Add(w.Stats);
                    }

                    // Lot-level stats aggregated from wafers
                    lotDto.Stats = AggregateStats(lotDto.Wafers.Select(w => w.Stats));
                    if (historyMap.TryGetValue(lot, out var h) && h is not null)
                        lotDto.Stats.HistoryInfo = h;

                    dto.Lots.Add(lotDto);
                }
            }

            dto.AllStats = BuildAllStats(allUnderlyingStats, test, specMap);
            results.Add(dto);
        }

        return results;
    }

    private static StatsDto AggregateStats(IEnumerable<StatsDto> src)
    {
        var list = src?.ToList() ?? new List<StatsDto>();
        if (list.Count == 0) return new StatsDto();

        return new StatsDto
        {
            Maximum = list.Max(s => s.Maximum),
            Minimum = list.Min(s => s.Minimum),
            Average = list.Average(s => s.Average),
            Median = Median(list.Select(s => s.Median)),
            DataCount = list.Sum(s => s.DataCount),
            Sigma = list.Average(s => s.Sigma),
            IQR = list.Average(s => s.IQR),
            HistoryInfo = new List<string>()
        };
    }

    private static AllStatsDto BuildAllStats(
        List<StatsDto> underlying,
        string testName,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap)
    {
        var all = new AllStatsDto();
        if (underlying.Count == 0)
            return all;

        all.Maximum = underlying.Max(s => s.Maximum);
        all.Minimum = underlying.Min(s => s.Minimum);
        all.Average = underlying.Average(s => s.Average);
        all.Median = Median(underlying.Select(s => s.Median));
        all.DataCount = underlying.Sum(s => s.DataCount);
        all.Sigma = underlying.Average(s => s.Sigma);
        all.IQR = underlying.Average(s => s.IQR);

        all.Min_Average = underlying.Min(s => s.Average);
        all.Max_Average = underlying.Max(s => s.Average);
        all.Min_Median = underlying.Min(s => s.Median);
        all.Max_Median = underlying.Max(s => s.Median);

        if (specMap.TryGetValue(testName, out var sp))
        {
            all.PlsSPEC = sp.upperSpec;
            all.MisSPEC = sp.lowerSpec;
            all.PlsLimit = sp.upperLimit;
            all.MisLimit = sp.lowerLimit;
        }

        return all;
    }

    private static async Task<Dictionary<string, List<string>>> LoadLotHistoryInfoMapAsync(
        DBAccess.DBAccessClient client,
        string proc,
        List<string> lotNames,
        CancellationToken ct)
    {
        var map = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        if (lotNames.Count == 0 || string.IsNullOrWhiteSpace(proc))
            return map;

        string sqlKey = proc.Equals("2PC", StringComparison.OrdinalIgnoreCase)
            ? "MEASURE_LOT_HISTORY_2PC_EQUIPPROG"
            : "MEASURE_LOT_HISTORY_1PC_EQUIPPROG";

        foreach (var lot in lotNames)
        {
            if (string.IsNullOrWhiteSpace(lot)) continue;

            try
            {
                var rid = Guid.NewGuid().ToString("N");
                var prms = new Dictionary<string, string> { ["lotName"] = lot };

                await client.OpenQueryAsync(new OpenQueryRequest
                {
                    ResultId = rid,
                    SqlKey = sqlKey,
                    Parameters = { prms }
                }, cancellationToken: ct);

                var vals = await FetchAllValuesAsync(client, rid, ct);

                var list = new List<string>();
                foreach (var v in vals)
                {
                    var parts = (v ?? "").Split('\t');
                    if (parts.Length < 2) continue;

                    var equipment = parts[0].Trim();
                    var program = parts[1].Trim();

                    if (string.IsNullOrWhiteSpace(equipment) && string.IsNullOrWhiteSpace(program))
                        continue;

                    list.Add($"{equipment}|{program}");
                }

                map[lot] = list.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            }
            catch
            {
                // ignore best-effort
            }
        }
        return map;
    }

    private static List<string>? ParseHistoryInfo(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        raw = raw.Trim();

        // If already JSON array, parse it.
        if (raw.StartsWith("["))
        {
            try
            {
                var list = JsonSerializer.Deserialize<List<string>>(raw);
                if (list is not null && list.Count > 0)
                    return list;
            }
            catch
            {
                // fall through to delimiter split
            }
        }

        // Accept common delimiters used by DB outputs or logs.
        var parts = raw.Split(new[] { ';', ',', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
                       .Select(x => x.Trim())
                       .Where(x => x.Length > 0)
                       .ToList();

        return parts.Count == 0 ? null : parts;
    }

    /// <summary>
    /// Parse a single string value from a flexible input representation.
    /// Accepts:
    ///  - plain string
    ///  - JSON array string (takes the first element)
    ///  - CSV / delimiter separated string (takes the first token)
    /// </summary>
    private static string ParseSingleStringFlexible(string raw)
    {
        raw ??= string.Empty;
        raw = raw.Trim();
        if (raw.Length == 0)
            return string.Empty;

        // JSON array -> take first
        if (raw.StartsWith("["))
        {
            try
            {
                var list = JsonSerializer.Deserialize<List<string>>(raw);
                var first = list?.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x));
                return first?.Trim() ?? string.Empty;
            }
            catch
            {
                // fall through
            }
        }

        // Delimiter split -> take first
        var token = raw.Split(new[] { ';', ',', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
                       .Select(x => x.Trim())
                       .FirstOrDefault(x => x.Length > 0);

        return token ?? string.Empty;
    }

    private static async Task<Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)>> LoadSpecLimitMapAsync(
        DBAccess.DBAccessClient client,
        string mainLot,
        List<string> testNames,
        CancellationToken ct)
    {
        var all = new Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(mainLot) || testNames is null || testNames.Count == 0)
            return all;

        foreach (var testName in testNames.Where(t => !string.IsNullOrWhiteSpace(t)).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            try
            {
                var rid = Guid.NewGuid().ToString("N");
                var prms = new Dictionary<string, string>
                {
                    ["lotName"] = mainLot,
                    ["testName"] = testName
                };

                await client.OpenQueryAsync(new OpenQueryRequest
                {
                    ResultId = rid,
                    SqlKey = "TEST_ITEMS_SPEC_LIMIT",
                    Parameters = { prms }
                }, cancellationToken: ct);

                var vals = await FetchAllValuesAsync(client, rid, ct);
                if (vals.Count == 0) continue;

                // Value: UPPER_SPEC \t LOWER_SPEC \t UPPER_LIMIT \t LOWER_LIMIT
                var parts = (vals[0] ?? "").Split('\t');
                if (parts.Length < 4) continue;

                var upperSpec = ToDouble(parts[0]);
                var lowerSpec = ToDouble(parts[1]);
                var upperLimit = ToDouble(parts[2]);
                var lowerLimit = ToDouble(parts[3]);

                all[testName] = (upperSpec, lowerSpec, upperLimit, lowerLimit);
            }
            catch
            {
                // best-effort
            }
        }

        return all;
    }

    private static StatisticsInfoDto BuildStatisticsInfo(
        IEnumerable<GraphPointData5Dto?> points,
        string itemName,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap)
    {
        var list = points.Where(p => p is not null).Cast<GraphPointData5Dto>().ToList();
        var info = new StatisticsInfoDto();
        if (list.Count == 0) return info;

        info.Minimum = list.Min(p => p.Minimum);
        info.Maximum = list.Max(p => p.Maximum);
        info.Average = list.Average(p => p.Average);
        info.Median = Median(list.Select(p => p.Median));
        info.DataCount = list.Sum(p => p.DataCount);
        info.Sigma = list.Average(p => p.Sigma);
        info.IQR = list.Average(p => p.IQR);
        info.Min_Average = list.Min(p => p.Average);
        info.Max_Average = list.Max(p => p.Average);
        info.Min_Median = list.Min(p => p.Median);
        info.Max_Median = list.Max(p => p.Median);

        if (specMap.TryGetValue(itemName, out var sp))
        {
            info.PlsSPECMainLot = sp.upperSpec;
            info.MisSPECMainLot = sp.lowerSpec;
            info.PlsLIMITMainLot = sp.upperLimit;
            info.MisLIMITMainLot = sp.lowerLimit;
        }

        return info;
    }

    private static GraphPointData5Dto AggregatePoint(List<GraphPointData5Dto> list)
        => new()
        {
            Minimum = list.Min(p => p.Minimum),
            Maximum = list.Max(p => p.Maximum),
            Average = list.Average(p => p.Average),
            Median = Median(list.Select(p => p.Median)),
            DataCount = list.Sum(p => p.DataCount),
            Sigma = list.Average(p => p.Sigma),
            IQR = list.Average(p => p.IQR),
            HistoryInfo = null
        };

    
    // =========================
    // RequestId=6 (Spec: Histogram)
    // =========================

    private static List<HistGraphDataDto> BuildHistGraphDataDto6(
        List<string> waferIdMapRows,
        List<string> chipMeasureRows,
        List<string> expectedLotNames,
        List<string> expectedTestNames,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap,
        Dictionary<string, List<string>> historyMap)
    {
        // WAFER_ID_MAP rows (tab-separated):
        //  LOT_NAME, TEST_NAME, WAFER_NO, WAFER_ID
        //
        // CHIP_MEASURE_DATA_(1PC/2PC) rows (tab-separated):
        //  LOT_NAME, TEST_NAME, WAFER_NO, CHIP_NO, RESULT

        // ---- Parse wafer id map
        var waferIdMap = new Dictionary<(string lot, string item, int waferNo), string>(WaferNoKeyComparer.Instance);
        foreach (var r in waferIdMapRows ?? new List<string>())
        {
            if (string.IsNullOrWhiteSpace(r)) continue;
            var p = r.Split('\t');
            if (p.Length < 4) continue;

            var lot = (p[0] ?? "").Trim();
            var item = (p[1] ?? "").Trim();
            if (!int.TryParse((p[2] ?? "").Trim(), out var waferNo)) waferNo = 0;
            var waferId = (p[3] ?? "").Trim();

            if (string.IsNullOrWhiteSpace(lot) || string.IsNullOrWhiteSpace(item) || waferNo <= 0)
                continue;

            waferIdMap[(lot, item, waferNo)] = waferId;
        }

        // ---- Parse chip measurements
        var chipMap = new Dictionary<(string lot, string item, int waferNo, int chipNo), List<double>>(ChipKeyComparer.Instance);
        foreach (var r in chipMeasureRows ?? new List<string>())
        {
            if (string.IsNullOrWhiteSpace(r)) continue;
            var p = r.Split('\t');
            if (p.Length < 5) continue;

            var lot = (p[0] ?? "").Trim();
            var item = (p[1] ?? "").Trim();
            if (!int.TryParse((p[2] ?? "").Trim(), out var waferNo)) waferNo = 0;
            if (!int.TryParse((p[3] ?? "").Trim(), out var chipNo)) chipNo = 0;
            if (!double.TryParse((p[4] ?? "").Trim(), out var result))
            {
                // tolerate locale
                if (!double.TryParse((p[4] ?? "").Trim(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out result))
                    continue;
            }

            if (string.IsNullOrWhiteSpace(lot) || string.IsNullOrWhiteSpace(item) || waferNo <= 0 || chipNo <= 0)
                continue;

            var key = (lot, item, waferNo, chipNo);
            if (!chipMap.TryGetValue(key, out var list))
            {
                list = new List<double>();
                chipMap[key] = list;
            }
            list.Add(result);
        }

        // ---- Build result (1 row per TEST_NAME, includes all lots)
        var results = new List<HistGraphDataDto>(capacity: expectedTestNames.Count);

        foreach (var test in expectedTestNames)
        {
            var dto = new HistGraphDataDto
            {
                ItemName = test,
                Lots = new List<LotDto>()
            };

            var allWaferStats = new List<StatsDto>(capacity: 256);

            foreach (var lot in expectedLotNames)
            {
                var hist = historyMap.TryGetValue(lot, out var h) && h is not null ? h : new List<string>();

                var lotDto = new LotDto
                {
                    LotName = lot,
                    Stats = new StatsDto { HistoryInfo = hist },
                    Wafers = new List<WaferDto>()
                };

                // waferNos found by chips for this (lot,test)
                var waferNos = chipMap.Keys
                    .Where(k => k.lot.Equals(lot, StringComparison.OrdinalIgnoreCase)
                             && k.item.Equals(test, StringComparison.OrdinalIgnoreCase))
                    .Select(k => k.waferNo)
                    .Distinct()
                    .OrderBy(x => x)
                    .ToList();

                foreach (var waferNo in waferNos)
                {
                    var waferId = waferIdMap.TryGetValue((lot, test, waferNo), out var wid) && !string.IsNullOrWhiteSpace(wid)
                        ? wid
                        : waferNo.ToString();

                    // chips under this wafer
                    var chipsForWafer = chipMap
                        .Where(kv => kv.Key.lot.Equals(lot, StringComparison.OrdinalIgnoreCase)
                                  && kv.Key.item.Equals(test, StringComparison.OrdinalIgnoreCase)
                                  && kv.Key.waferNo == waferNo)
                        .GroupBy(kv => kv.Key.chipNo)
                        .OrderBy(g => g.Key)
                        .ToList();

                    var waferAllValues = new List<double>();

                    var waferDto = new WaferDto
                    {
                        WaferId = waferId,
                        Stats = new StatsDto { HistoryInfo = hist },
                        Chips = new List<ChipDto>()
                    };

                    foreach (var cg in chipsForWafer)
                    {
                        var values = cg.SelectMany(x => x.Value).Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToList();
                        waferAllValues.AddRange(values);

                        var cStats = BuildStatsFromValues(values);
                        cStats.HistoryInfo = hist;

                        waferDto.Chips.Add(new ChipDto
                        {
                            ChipId = cg.Key,
                            Stats = cStats
                        });
                    }

                    // Wafer stats are computed from all chip results under that wafer
                    var wStats = BuildStatsFromValues(waferAllValues);
                    wStats.HistoryInfo = hist;
                    waferDto.Stats = wStats;

                    lotDto.Wafers.Add(waferDto);

                    if (waferDto.Stats is not null && waferDto.Stats.DataCount > 0)
                        allWaferStats.Add(waferDto.Stats);
                }

                // Lot stats are derived from wafer stats (unweighted; count = number of wafers)
                lotDto.Stats = AggregateStatsFromWaferStats(lotDto.Wafers.Select(w => w.Stats), hist);

                dto.Lots.Add(lotDto);
            }

            // AllStats are derived from all wafer stats across all lots (unweighted; count = number of wafers)
            dto.AllStats = BuildAllStatsFromWaferStats(allWaferStats, test, specMap);
            results.Add(dto);
        }

        return results;
    }

    private static StatsDto AggregateStatsFromWaferStats(IEnumerable<StatsDto> waferStats, List<string> historyInfo)
    {
        var list = (waferStats ?? Enumerable.Empty<StatsDto>())
            .Where(s => s is not null && s.DataCount > 0)
            .ToList();

        if (list.Count == 0)
            return new StatsDto { HistoryInfo = historyInfo };

        return new StatsDto
        {
            Maximum = list.Max(p => p.Maximum),
            Minimum = list.Min(p => p.Minimum),
            Average = list.Average(p => p.Average),
            Median = list.Average(p => p.Median),
            DataCount = list.Count,
            Sigma = list.Average(p => p.Sigma),
            IQR = list.Average(p => p.IQR),
            HistoryInfo = historyInfo
        };
    }

    private static AllStatsDto BuildAllStatsFromWaferStats(
        List<StatsDto> waferStats,
        string testName,
        Dictionary<string, (double upperSpec, double lowerSpec, double upperLimit, double lowerLimit)> specMap)
    {
        var list = (waferStats ?? new List<StatsDto>())
            .Where(s => s is not null && s.DataCount > 0)
            .ToList();

        var all = new AllStatsDto();

        if (list.Count > 0)
        {
            all.Maximum = list.Max(p => p.Maximum);
            all.Minimum = list.Min(p => p.Minimum);
            all.Average = list.Average(p => p.Average);
            all.Median = list.Average(p => p.Median);
            all.DataCount = list.Count;
            all.Sigma = list.Average(p => p.Sigma);
            all.IQR = list.Average(p => p.IQR);

            all.Min_Average = list.Min(p => p.Average);
            all.Max_Average = list.Max(p => p.Average);
            all.Min_Median = list.Min(p => p.Median);
            all.Max_Median = list.Max(p => p.Median);
        }

        if (specMap.TryGetValue(testName, out var s))
        {
            all.PlsSPEC = s.upperSpec;
            all.MisSPEC = s.lowerSpec;
            all.PlsLimit = s.upperLimit;
            all.MisLimit = s.lowerLimit;
        }

        return all;
    }

    private sealed class WaferNoKeyComparer : IEqualityComparer<(string lot, string item, int waferNo)>
    {
        public static readonly WaferNoKeyComparer Instance = new();

        public bool Equals((string lot, string item, int waferNo) x, (string lot, string item, int waferNo) y)
            => string.Equals(x.lot, y.lot, StringComparison.OrdinalIgnoreCase)
               && string.Equals(x.item, y.item, StringComparison.OrdinalIgnoreCase)
               && x.waferNo == y.waferNo;

        public int GetHashCode((string lot, string item, int waferNo) obj)
            => HashCode.Combine(obj.lot.ToLowerInvariant(), obj.item.ToLowerInvariant(), obj.waferNo);
    }
private static StatsDto BuildStatsFromValues(List<double> values)
    {
        var xs = values?.Where(v => !double.IsNaN(v) && !double.IsInfinity(v)).ToList() ?? new List<double>();
        if (xs.Count == 0) return new StatsDto();

        xs.Sort();
        var avg = xs.Average();
        var variance = xs.Count == 0 ? 0 : xs.Select(v => (v - avg) * (v - avg)).Average();
        var sigma = Math.Sqrt(variance);

        return new StatsDto
        {
            Minimum = xs.First(),
            Maximum = xs.Last(),
            Average = avg,
            Median = Median(xs),
            DataCount = xs.Count,
            Sigma = sigma,
            IQR = ComputeIqr(xs),
            HistoryInfo = new List<string>()
        };
    }

    private static double ComputeIqr(List<double> sortedValues)
    {
        if (sortedValues is null || sortedValues.Count == 0) return 0;

        // sortedValues must be sorted
        double MedianOfRange(int start, int count)
        {
            var slice = sortedValues.Skip(start).Take(count).ToList();
            return Median(slice);
        }

        var n = sortedValues.Count;
        if (n == 1) return 0;

        var half = n / 2;
        double q1, q3;
        if (n % 2 == 0)
        {
            q1 = MedianOfRange(0, half);
            q3 = MedianOfRange(half, half);
        }
        else
        {
            q1 = MedianOfRange(0, half);
            q3 = MedianOfRange(half + 1, half);
        }

        return q3 - q1;
    }

    private sealed class ChipKeyComparer : IEqualityComparer<(string lot, string item, int waferNo, int chipNo)>
    {
        public static readonly ChipKeyComparer Instance = new();

        public bool Equals((string lot, string item, int waferNo, int chipNo) x, (string lot, string item, int waferNo, int chipNo) y)
            => string.Equals(x.lot, y.lot, StringComparison.OrdinalIgnoreCase)
               && string.Equals(x.item, y.item, StringComparison.OrdinalIgnoreCase)
               && x.waferNo == y.waferNo
               && x.chipNo == y.chipNo;

        public int GetHashCode((string lot, string item, int waferNo, int chipNo) obj)
            => HashCode.Combine(obj.lot.ToLowerInvariant(), obj.item.ToLowerInvariant(), obj.waferNo, obj.chipNo);
    }



private static int ToInt(string? s)
{
    if (string.IsNullOrWhiteSpace(s)) return 0;

    if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return v;
    if (int.TryParse(s, NumberStyles.Integer, CultureInfo.CurrentCulture, out v)) return v;

    // Some DBs may output numeric values as floating string; fall back.
    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return (int)d;
    if (double.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out d)) return (int)d;

    return 0;
}

private static double ToDouble(string? s)
{
    if (string.IsNullOrWhiteSpace(s)) return 0d;

    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v)) return v;
    if (double.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out v)) return v;

    return 0d;
}

private static double Median(IEnumerable<double> src)
    {
        var arr = src.OrderBy(x => x).ToArray();
        if (arr.Length == 0) return 0;
        var mid = arr.Length / 2;
        return arr.Length % 2 == 1 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2.0;
    }

    private sealed record ParsedStatRow(
        string Lot,
        string Item,
        int? WaferNo,
        double Min,
        double Max,
        double Avg,
        double Med,
        int Cnt,
        double Std,
        double Iqr,
        string History);

    private sealed record ParsedChipRow(
        string Lot,
        string Item,
        int WaferNo,
        float Result);

    private sealed class StringTupleComparer : IEqualityComparer<(string Lot, string Item)>
    {
        public static readonly StringTupleComparer Instance = new();

        public bool Equals((string Lot, string Item) x, (string Lot, string Item) y)
            => string.Equals(x.Lot, y.Lot, StringComparison.OrdinalIgnoreCase)
               && string.Equals(x.Item, y.Item, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Lot, string Item) obj)
            => HashCode.Combine(obj.Lot.ToLowerInvariant(), obj.Item.ToLowerInvariant());
    }

    public async Task CancelAsync(RequestState state, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(state.ResultId))
            return;

        try
        {
            using var ch = GrpcChannel.ForAddress(_addrs.DbAccessAddress, GrpcChannelFactory.Options());
            var client = new DBAccess.DBAccessClient(ch);
            await client.CancelQueryAsync(new CancelQueryRequest { ResultId = state.ResultId }, cancellationToken: cancellationToken);
        }
        catch
        {
            // best-effort
        }
    }
}

internal static class GrpcChannelFactory
{
    public static GrpcChannelOptions Options() => new()
    {
        HttpHandler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true }
    };
}