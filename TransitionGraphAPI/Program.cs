// NuGetインストール
using Google.Protobuf.WellKnownTypes;
// NuGetインストール
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using TransitionGraph.Dtos;
using TransitionGraph.Shared;
using TransitionGraph.V1;
using TransitionGraphAPI;
using TransitionGraph.LogOutPut;
using TransitionGraph.PortSetting;

// Avoid enum name ambiguity between DTO and gRPC
using DtoRequestResultStatus = TransitionGraph.Dtos.RequestResultStatus;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// One-time diagnostic guard
int _gridAsmDiagOnce = 0;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

// -------------------------------
// Port setting (PortSetting.properties)
// -------------------------------
var ports = PortSettingStore.Current;
var listenPort = ports.ApiServerPort;

// Allow passing --port 5000
for (int i = 0; i + 1 < args.Length; i++)
{
    if (args[i].Equals("--port", StringComparison.OrdinalIgnoreCase) && int.TryParse(args[i + 1], out var p))
        listenPort = p;
}

// IMPORTANT:
// Do not hard-bind Kestrel endpoints so that standard hosting configuration works:
// - command line: --urls http://127.0.0.1:5000
// - env: ASPNETCORE_URLS
// If neither is provided, fall back to PortSetting.properties / --port.
builder.WebHost.ConfigureKestrel(o =>
{
    o.ConfigureEndpointDefaults(lo => lo.Protocols = HttpProtocols.Http1AndHttp2);
});

var hasUrlsArg = args.Any(a => a.Equals("--urls", StringComparison.OrdinalIgnoreCase)) ||
                 args.Any(a => a.StartsWith("--urls=", StringComparison.OrdinalIgnoreCase));
var hasEnvUrls = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ASPNETCORE_URLS"));
if (!hasUrlsArg && !hasEnvUrls)
{
    builder.WebHost.UseUrls($"{ports.Scheme}://{ports.Host}:{listenPort}");
}

// Allow passing --dataDir "path"
if (args.Length >= 2 && args[0] == "--dataDir")
{
    builder.Configuration["App:DataDir"] = args[1];
}

builder.Services.AddResponseCompression();

// gRPC clients (plaintext localhost)
static GrpcChannel MakeChannel(string address)
    => GrpcChannel.ForAddress(address, new GrpcChannelOptions
    {
        HttpHandler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true
        }
    });

// Worker manager (spawns child processes)
builder.Services.AddSingleton<WorkerManager>();

// 全件取得した結果をメモリ保持し、/result でページング返却する
builder.Services.AddSingleton<ResultSetMemoryStore>();

var app = builder.Build();
app.UseResponseCompression();

// Fetch件数をappsetting.jsonより取り出す
var pageSize = builder.Configuration.GetValue<int?>("Paging:PageSize") ?? 500;
// 変な設定値なら５００にする
if(pageSize < 500) pageSize = 500;
if(pageSize > 2000) pageSize = 500;

var dataDir = app.Configuration.GetValue<string>("App:DataDir")
             ?? Path.Combine(AppContext.BaseDirectory, "data");
dataDir = Path.GetFullPath(dataDir);
Directory.CreateDirectory(dataDir);

// ============================================================
// 取得データ確認用の簡易ログ出力
//   - /result で返却したデータを dataDir/logs 配下に追記する
//   - 値が長い場合はログ肥大化を防ぐため 200 文字で省略する
// ============================================================
var dumpEnabled = app.Configuration.GetValue<bool>("ResultDump:Enabled", false);
var dumpMaxRowsPerCall = app.Configuration.GetValue<int>("ResultDump:MaxRowsPerCall", 200);
if (dumpMaxRowsPerCall < 1) dumpMaxRowsPerCall = 1;
if (dumpMaxRowsPerCall > 5000) dumpMaxRowsPerCall = 5000;

static string TruncateForLog(string? s, int maxLen = 200)
{
    if (s is null) return "";
    if (s.Length <= maxLen) return s;
    return s.Substring(0, maxLen) + " ...";
}

void AppendResultDump(string resultId, int lastRowNo, int returnedCount, bool done, IEnumerable<Point> points)
{
    if (!dumpEnabled) return;
    try
    {
        var logDir = Path.Combine(dataDir, "logs");
        Directory.CreateDirectory(logDir);
        var logPath = Path.Combine(logDir, $"result_{resultId}.log");

        var now = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
        using var sw = new StreamWriter(logPath, append: true);
        sw.WriteLine($"[{now}] /result called  lastRowNo={lastRowNo} returned={returnedCount} done={done}");

        int i = 0;
        foreach (var p in points)
        {
            if (i++ >= dumpMaxRowsPerCall)
            {
                sw.WriteLine($"  ...(truncated. max {dumpMaxRowsPerCall} rows per call)");
                break;
            }
            sw.WriteLine($"  RowNo={p.RowNo} Value={TruncateForLog(p.Value)}");
        }

        sw.WriteLine();
    }
    catch
    {
        // ログ出力失敗は業務処理を止めない
    }
}

var workers = app.Services.GetRequiredService<WorkerManager>();
var memStore = app.Services.GetRequiredService<ResultSetMemoryStore>();

await workers.StartAsync(app.Configuration, dataDir);

// POST /query => TransitionGraphDataEdit (gRPC) がリクエストを受け付け、
// DBAccess などの下位処理を行い ResultStore(dataDir/*.csv) に保存する。
// (この段階ではキャッシュやキャンセルは未対応)
// Spec (4-1): request body is RequestHeaderDto (common header)
app.MapPost("/api/transitiongraph/query", async (RequestHeaderDto header) =>
{
    MessageOut.MessageOutPut("TransitionGraphAPI リクエスト受信");
    // 仮処理（確認用）
    if (header.RequestId == "3")
    {
        return Results.Ok(new ResponseDto<RequestAnwerDto>
        {
            Header = header,
            Answer = new RequestAnwerDto { RequestAnswer = new { } }
        });
    }


    if (header.RequestId == "100")
    {
        // いまの実装に合わせて parameter[1] を採用（必要なら [0] に変更）
        var otherSystemHost = header.Parameter?.ElementAtOrDefault(1) ?? "";

        // 既存の環境変数保持は残してOK（ただし起動済みDataEditには効かない）
        Environment.SetEnvironmentVariable("TRANSITIONGRAPH_OTHER_SYSTEM_HOST", otherSystemHost);

        // ★追加：DataEditへ gRPC で RequestId=100 を転送して、DataEditプロセス内に保持させる
        try
        {
            using var _ch = MakeChannel(workers.DataEditAddress);
            var _client = new DataEdit.DataEditClient(_ch);

            var grpcReq100 = new BuildRequest
            {
                Header = new RequestHeader
                {
                    Guid = header.GUID ?? "",
                    RequestId = "100",
                    SequenceNo = header.SequenceNo ?? "",
                    RequestInstanceId = header.RequestInstanceId ?? "",
                    RequestResult = TransitionGraph.V1.RequestResultStatus.Ok
                },
                SqlKey = "100",
                TotalCount = 0
            };

            // DataEdit側で拾いやすいように Parm1 で渡す（既存のParmマッピング方針と同じ）
            grpcReq100.Parameters["Parm1"] = otherSystemHost;

            // これで DataEdit/Program.cs 側の「RequestId=100 ハンドラ」が Cache.Set できる
            await _client.BuildResultSetAsync(grpcReq100);

            // Alignmentへも同じ RequestId=100 を転送して、Alignmentプロセス内に保持させる
            // これにより RequestId=8/9 でも RequestId=100 のサーバアドレスを参照可能にする
            try
            {
                using var _alCh = MakeChannel(workers.AlignmentAddress);
                var _alClient = new Alignment.AlignmentClient(_alCh);
                await _alClient.BuildResultSetAsync(grpcReq100);
            }
            catch (Exception exAl)
            {
                MessageOut.MessageOutPut($"RequestId=100 notify Alignment failed: {exAl}");
            }
        }
        catch (Exception ex)
        {
            // 通知失敗しても API の応答自体は返す（ログにだけ残す）
            MessageOut.MessageOutPut($"RequestId=100 notify DataEdit failed: {ex}");
        }

        var respHeader = new RequestHeaderDto
        {
            GUID = header.GUID ?? "",
            RequestId = header.RequestId ?? "",
            SequenceNo = header.SequenceNo ?? "",
            Parameter = header.Parameter,
            RequestKey = header.RequestKey,
            RequestResult = DtoRequestResultStatus.OK,
            RequestInstanceId = header.RequestInstanceId
        };

        return Results.Ok(new ResponseDto<RequestAnwerDto>
        {
            Header = respHeader,
            Answer = new RequestAnwerDto { RequestAnswer = "Accept Request" }
        });
    }

    using var ch = MakeChannel(workers.DataEditAddress);
    var client = new DataEdit.DataEditClient(ch);

    // Default SqlKey
    var sqlKey = header.RequestId ?? "";

    // RequestId=5 は DataSort（Parameter[3]）でSQLを切替
    if (header.RequestId == "5")
    {
        var dataSort = header.Parameter?.ElementAtOrDefault(3) ?? "";
        sqlKey = dataSort.Equals("Lot", StringComparison.OrdinalIgnoreCase)
            ? "LOT_STATISTIC_DATA"
            : "WAFER_STATISTIC_DATA";
    }

    var grpcReq = new BuildRequest
    {
        Header = new RequestHeader
        {
            Guid = header.GUID ?? "",
            RequestId = header.RequestId ?? "",
            SequenceNo = header.SequenceNo ?? "",
            RequestInstanceId = header.RequestInstanceId ?? "",
            RequestResult = header.RequestResult switch
            {
                DtoRequestResultStatus.OK => TransitionGraph.V1.RequestResultStatus.Ok,
                DtoRequestResultStatus.NG => TransitionGraph.V1.RequestResultStatus.Ng,
                DtoRequestResultStatus.CANCEL => TransitionGraph.V1.RequestResultStatus.Cancel,
                _ => TransitionGraph.V1.RequestResultStatus.Running,
            }
        },
        SqlKey = sqlKey,
        TotalCount = 0
    };

    // Convert parameter list into grpc parameters dictionary
    // Special mapping for some RequestIds to match WPF spec.
    var handled = false;

    // RequestId=2: parameter[0]=process, [1]=startTime, [2]=endTime
    if (string.Equals(header.RequestId, "2", StringComparison.OrdinalIgnoreCase))
    {
        var proc = header.Parameter?.ElementAtOrDefault(0) ?? "";
        var startTime = header.Parameter?.ElementAtOrDefault(1) ?? "";
        var endTime = header.Parameter?.ElementAtOrDefault(2) ?? "";
        grpcReq.Parameters["process"] = proc;
        grpcReq.Parameters["startTime"] = startTime;
        grpcReq.Parameters["endTime"] = endTime;
        // Also keep Parm1.. for backward compatibility
    }

    // RequestId=4: parameter[0]=process, parameter[1..]=lot name list
    if (string.Equals(header.RequestId, "4", StringComparison.OrdinalIgnoreCase))
    {
        var proc = header.Parameter?.ElementAtOrDefault(0) ?? "";
        var lotNames = header.Parameter is null ? new List<string>() : header.Parameter.Skip(1).Where(x => !string.IsNullOrWhiteSpace(x)).ToList();
        grpcReq.Parameters["process"] = proc;
        grpcReq.Parameters["lotNamesJson"] = System.Text.Json.JsonSerializer.Serialize(lotNames);
        // Avoid expanding lot list into many Parm keys (screen flow keeps it as a list)
        if (header.Parameter is not null && header.Parameter.Count > 0)
        {
            grpcReq.Parameters["Parm1"] = header.Parameter[0] ?? "";
        }
        handled = true;
    }

    // Default: Parm1..ParmN mapping
    if (!handled && header.Parameter is not null)
    {
        for (var i = 0; i < header.Parameter.Count; i++)
        {
            grpcReq.Parameters[$"Parm{i + 1}"] = header.Parameter[i] ?? "";
        }
    }

    // RequestKey map (lot -> item list) into grpc header
    // Spec: header.reqKey.RequestKey
    var reqKeyMap = header.RequestKey;
    if (reqKeyMap is not null)
    {
        if (header.RequestId is "4" or "5" or "6")
        {
            // normalize order for stable cache hit
            var normalized = reqKeyMap
                .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    x => x.Key,
                    x => (x.Value ?? new List<string>())
                        .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
                        .ToList(),
                    StringComparer.OrdinalIgnoreCase
                );

            grpcReq.Parameters["reqKeyJson"] = System.Text.Json.JsonSerializer.Serialize(normalized);
        }

        foreach (var kv in reqKeyMap)
        {
            var v = new RequestKeyValue();
            if (kv.Value is not null)
                v.ItemName.AddRange(kv.Value);
            grpcReq.Header.RequestKey[kv.Key] = v;
        }
    }

    BuildReply? rep = null;

    // --- Diagnostic (once): confirm which DXC.EngPF.Core.Grid.dll is actually loaded ---
    // This is intentionally here (close to the failing call site) so the log appears even
    // when the request fails before downstream logging is emitted.
    if (System.Threading.Interlocked.Exchange(ref _gridAsmDiagOnce, 1) == 0)
    {
        try
        {
            // NOTE: This project doesn't reference Serilog's static Log.
            // Write to both our app logger and stderr so it appears even if logging config changes.
            void Diag(string msg)
            {
                try { MessageOut.MessageOutPut(msg); } catch { /* ignore */ }
                try { Console.Error.WriteLine(msg); } catch { /* ignore */ }
            }

            // ============================================
            //  Debug
            // ============================================
            Diag($"[GridDLL] BaseDir={AppContext.BaseDirectory}");
            Diag($"[Proc] Is64BitProcess={Environment.Is64BitProcess}");

            static string PeMachine(string path)
            {
                if (!File.Exists(path)) return "NOT_FOUND";

                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var br = new BinaryReader(fs);

                // DOS header
                if (br.ReadUInt16() != 0x5A4D) return "NOT_PE"; // "MZ"
                fs.Seek(0x3C, SeekOrigin.Begin);
                var lfanew = br.ReadInt32();

                // PE header
                fs.Seek(lfanew, SeekOrigin.Begin);
                if (br.ReadUInt32() != 0x00004550) return "NOT_PE"; // "PE\0\0"
                var machine = br.ReadUInt16();

                return machine switch
                {
                    0x014c => "x86 (IMAGE_FILE_MACHINE_I386)",
                    0x8664 => "x64 (IMAGE_FILE_MACHINE_AMD64)",
                    0xAA64 => "ARM64 (IMAGE_FILE_MACHINE_ARM64)",
                    _ => $"Unknown(0x{machine:X4})"
                };
            }

            // BaseDir直下と BaseDir\DLL を両方チェック（あなたの配置パターンに合わせる）
            var baseDir = AppContext.BaseDirectory;
            var p1 = Path.Combine(baseDir, "log4net.dll");
            var p2 = Path.Combine(baseDir, "DLL", "log4net.dll");

            Diag($"[log4net] File={p1} Machine={PeMachine(p1)}");
            Diag($"[log4net] File={p2} Machine={PeMachine(p2)}");
            // ============================================
            //  Debug
            // ============================================

            var loaded = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => string.Equals(a.GetName().Name, "DXC.EngPF.Core.Grid", StringComparison.OrdinalIgnoreCase))
                .ToArray();

            if (loaded.Length == 0)
            {
                Diag("[GridDLL] LoadedAssemblies: (none)");
            }
            else
            {
                Diag("[GridDLL] LoadedAssemblies:");
                foreach (var a in loaded)
                {
                    string? loc;
                    try { loc = a.Location; } catch { loc = null; }
                    Diag($"[GridDLL]   - {a.FullName} | Location={loc ?? "(null)"}");
                }
            }
        }
        catch (Exception ex)
        {
            try { MessageOut.MessageOutPut("[GridDLL] Diagnostic failed: " + ex); } catch { /* ignore */ }
            try { Console.Error.WriteLine("[GridDLL] Diagnostic failed: " + ex); } catch { /* ignore */ }
        }
    }

    try
    {
        rep = await client.BuildResultSetAsync(grpcReq);
    }
    catch (TaskCanceledException)
    {
        // Cancelled is an explicit user action. Other RpcExceptions should be treated as NG.
        header.RequestResult = DtoRequestResultStatus.NG;

        // RequestId 1-4,7,10 は “即時応答型” のため、その形式で返す
        if (header.RequestId is "1" or "2" or "3" or "4" or "7" or "10")
        {
            return Results.Ok(new ResponseDto<RequestAnwerDto>
            {
                Header = header,
                Answer = new RequestAnwerDto { RequestAnswer = new { } }
            });
        }

        // RequestId 5-6 は “ページング型” のため、その形式で返す
        return Results.Ok(new ResponseDto<QueryAnswerDto>
        {
            Header = header,
            Answer = new QueryAnswerDto
            {
                ResultId = "",
                Status = "Canceled",
                PageSize = pageSize,
                NextCursor = null
            }
        });
    }
    catch (OperationCanceledException ex)
    {
        // gRPC呼び出し待ち中にローカル側でキャンセルされたケース（通信断/RequestAborted等）
        header.RequestResult = DtoRequestResultStatus.CANCEL;

        Console.WriteLine("=== OperationCanceledException ===");
        Console.WriteLine(ex.ToString());

        if (header.RequestId is "1" or "2" or "3" or "4" or "7" or "10")
        {
            return Results.Ok(new ResponseDto<RequestAnwerDto>
            {
                Header = header,
                Answer = new RequestAnwerDto { RequestAnswer = new { } }
            });
        }

        return Results.Ok(new ResponseDto<QueryAnswerDto>
        {
            Header = header,
            Answer = new QueryAnswerDto
            {
                ResultId = "",
                Status = "Canceled",
                PageSize = pageSize,
                NextCursor = null
            }
        });
    }
    //catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.Cancelled)
    catch (Grpc.Core.RpcException ex)
    {
        // gRPC 側のキャンセル判定はここで行う
        header.RequestResult = ex.StatusCode == Grpc.Core.StatusCode.Cancelled
            ? DtoRequestResultStatus.CANCEL
            : DtoRequestResultStatus.NG;

        Console.WriteLine("=== gRPC RpcException ===");
        Console.WriteLine($"StatusCode : {ex.StatusCode}");
        Console.WriteLine($"Detail     : {ex.Status.Detail}");
        Console.WriteLine($"Exception  : {ex.ToString()}");

        MessageOut.MessageOutPut("=== gRPC RpcException ===");
        MessageOut.MessageOutPut($"StatusCode : {ex.StatusCode}");
        MessageOut.MessageOutPut($"Detail     : {ex.Status.Detail}");
        MessageOut.MessageOutPut($"Exception  : {ex.ToString()}");

        // Return the gRPC error detail to the caller for easier troubleshooting.
        // Keep response shape consistent with other immediate-answer RequestIds.
        return Results.Ok(new ResponseDto<RequestAnwerDto>
        {
            Header = header,
            Answer = new RequestAnwerDto
            {
                RequestAnswer = new
                {
                    error = ex.Status.Detail,
                    statusCode = ex.StatusCode.ToString()
                }
            }
        });
    }
    catch (Exception ex)
    {
        header.RequestResult = DtoRequestResultStatus.NG;

        Console.WriteLine("=== Unexpected Exception start ===");
        Console.WriteLine(ex.ToString());
        Console.WriteLine("=== Unexpected Exception end ===");
        MessageOut.MessageOutPut("=== Unexpected Exception start ===");
        MessageOut.MessageOutPut(ex.ToString());
        MessageOut.MessageOutPut("=== Unexpected Exception end ===");

        return Results.Ok(new { header, answer = new { requestAnswer = new { } } });
    }

    // Spec: For RequestId 1-4,7,10 return header + requestAnswer directly (no paging).
    // - ID=1 : process list
    // - ID=2 : lot->item dictionary (stored as JSON in ResultStore)
    // - ID=3 : 欠番 (return empty object)
    if (header.RequestId is "1" or "2" or "3" or "4" or "7" or "10")
    {
        if (rep != null)
        {
            var storePath = ResultStore.GetPath(dataDir, rep.ResultId);

            // Best-effort: read the full answer written by TransitionGraphDataEdit.
            // (ID=1/2 are small)
            try
            {
                if (header.RequestId == "1")
                {
                    var lines = File.Exists(storePath) ? File.ReadAllLines(storePath) : Array.Empty<string>();
                    var list = new List<string>();
                    foreach (var line in lines)
                    {
                        var idx = line.IndexOf(',');
                        if (idx <= 0) continue;
                        var v = line[(idx + 1)..];
                        if (!string.IsNullOrWhiteSpace(v)) list.Add(v);
                    }

                    // cleanup (not cached)
                    try { if (File.Exists(storePath)) File.Delete(storePath); } catch { /* ignore */ }

                    return Results.Ok(new ResponseDto<RequestAnwerDto>
                    {
                        Header = header,
                        Answer = new RequestAnwerDto { RequestAnswer = new WorkProcessDto { ProcessName = list } }
                    });
                }

                if (header.RequestId == "2")
                {
                    string? json = null;
                    if (File.Exists(storePath))
                    {
                        // stored as one line: 1,<json>
                        var first = File.ReadLines(storePath).FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(first))
                        {
                            var idx = first.IndexOf(',');
                            if (idx >= 0 && idx + 1 < first.Length)
                                json = first[(idx + 1)..];
                        }
                    }

                    var dict = string.IsNullOrWhiteSpace(json)
                        ? new Dictionary<string, List<string>>()
                        : System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, List<string>>>(json!)
                          ?? new Dictionary<string, List<string>>();

                    // cleanup (not cached)
                    try { if (File.Exists(storePath)) File.Delete(storePath); } catch { /* ignore */ }

                    return Results.Ok(new ResponseDto<RequestAnwerDto>
                    {
                        Header = header,
                        Answer = new RequestAnwerDto { RequestAnswer = new LotItemNameDto { Lot_Item_Name = dict } }
                    });
                }

                if (header.RequestId == "4")
                {
                    // stored as one line: 1,<json>
                    string? json = null;
                    if (File.Exists(storePath))
                    {
                        var first = File.ReadLines(storePath).FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(first))
                        {
                            var idx = first.IndexOf(',');
                            if (idx >= 0 && idx + 1 < first.Length)
                                json = first[(idx + 1)..];
                        }
                    }

                    var lotInfo = string.IsNullOrWhiteSpace(json)
                        ? new LotInformationDto()
                        : System.Text.Json.JsonSerializer.Deserialize<LotInformationDto>(json!)
                          ?? new LotInformationDto();

                    // cleanup (cache will keep for 4-6; but for ID=4 direct answer, file is still used by cache)
                    // Do NOT delete storePath here.

                    return Results.Ok(new ResponseDto<RequestAnwerDto>
                    {
                        Header = header,
                        Answer = new RequestAnwerDto { RequestAnswer = lotInfo }
                    });
                }

                if (header.RequestId == "7")
                {
                    // stored as one line: 1,<json>
                    string? json = null;
                    if (File.Exists(storePath))
                    {
                        var first = File.ReadLines(storePath).FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(first))
                        {
                            var idx = first.IndexOf(',');
                            if (idx >= 0 && idx + 1 < first.Length)
                                json = first[(idx + 1)..];
                        }
                    }

                    var istar = string.IsNullOrWhiteSpace(json)
                        ? new ISTARInfoDto()
                        : System.Text.Json.JsonSerializer.Deserialize<ISTARInfoDto>(json!)
                          ?? new ISTARInfoDto();

                    // cleanup (not cached)
                    try { if (File.Exists(storePath)) File.Delete(storePath); } catch { /* ignore */ }

                    return Results.Ok(new ResponseDto<RequestAnwerDto>
                    {
                        Header = header,
                        Answer = new RequestAnwerDto { RequestAnswer = istar }
                    });
                }

                if (header.RequestId == "10")
                {
                    // stored as one line: 1,<string>
                    string? value = null;
                    if (File.Exists(storePath))
                    {
                        var first = File.ReadLines(storePath).FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(first))
                        {
                            var idx = first.IndexOf(',');
                            if (idx >= 0 && idx + 1 < first.Length)
                                value = first[(idx + 1)..];
                        }
                    }

                    // cleanup (not cached)
                    try { if (File.Exists(storePath)) File.Delete(storePath); } catch { /* ignore */ }

                    return Results.Ok(new ResponseDto<RequestAnwerDto>
                    {
                        Header = header,
                        Answer = new RequestAnwerDto { RequestAnswer = value ?? "Accept Request" }
                    });
                }
            }
            catch
            {
                // fall-through to legacy paging response if reading/parsing failed
            }

            // ID=3 (欠番) or fallback
            return Results.Ok(new ResponseDto<RequestAnwerDto>
            {
                Header = header,
                Answer = new RequestAnwerDto { RequestAnswer = new { } }
            });
        }
        else
        {
            return Results.Ok(new { header, answer = new { requestAnswer = new { } } });
        }
    }

    var cursor = Cursor.Encode(new Cursor(0));
    return Results.Ok(new ResponseDto<QueryAnswerDto>
    {
        Header = header,
        Answer = new QueryAnswerDto
        {
            ResultId = rep.ResultId,
            Status = "Ready",
            PageSize = pageSize,
            NextCursor = cursor,
        }
    });
});

// POST /cancel => cancel a running request in TransitionGraphDataEdit.
// The client should pass header.requestInstanceId when possible.
app.MapPost("/api/transitiongraph/cancel", async (CancelRequestDto req) =>
{
    using var ch = MakeChannel(workers.DataEditAddress);
    var client = new DataEdit.DataEditClient(ch);

    var grpcReq = new CancelRequest
    {
        Header = new RequestHeader
        {
            Guid = req.Header?.GUID ?? "",
            RequestId = req.Header?.RequestId ?? "",
            SequenceNo = req.Header?.SequenceNo ?? "",
            RequestInstanceId = req.Header?.RequestInstanceId ?? "",
            RequestResult = req.Header?.RequestResult switch
            {
                DtoRequestResultStatus.OK => TransitionGraph.V1.RequestResultStatus.Ok,
                DtoRequestResultStatus.NG => TransitionGraph.V1.RequestResultStatus.Ng,
                DtoRequestResultStatus.CANCEL => TransitionGraph.V1.RequestResultStatus.Cancel,
                _ => TransitionGraph.V1.RequestResultStatus.Running,
            }
        },
        ResultId = req.ResultId ?? ""
    };

    var rep = await client.CancelAsync(grpcReq);

    return Results.Ok(new ResponseDto<CancelAnswerDto>
    {
        Header = req.Header ?? new RequestHeaderDto(),
        Answer = new CancelAnswerDto
        {
            Canceled = rep.Canceled,
            Message = rep.Message
        }
    });
});


// GET /result/{resultId}?cursor=...
app.MapGet("/api/transitiongraph/result/{resultId}", async (string resultId, string? cursor) =>
{
    var c = Cursor.Decode(cursor);

    // Primary: ResultStore (written by TransitionGraphDataEdit)
    var storePath = ResultStore.GetPath(dataDir, resultId);
    if (File.Exists(storePath))
    {
        var tuples = ResultStore.ReadPage(dataDir, resultId, c.LastRowNo, pageSize,
            out var nextLastRowNo, out var done);

        var nextCursor2 = done ? null : Cursor.Encode(new Cursor(nextLastRowNo));
        var points = tuples.Select(t => new Point { RowNo = t.RowNo, Value = t.Value }).ToList();

        AppendResultDump(resultId, c.LastRowNo, points.Count, done, points);

        var items2 = points.Select(p => new { rowNo = p.RowNo, value = p.Value }).Cast<object>().ToList();
        return Results.Ok(new ResultResponse(items2, nextCursor2, done, points.Count));
    }

    // /query で取得済みの全件結果から pageSize ごとに分割して返却
    if (memStore.TryGet(resultId, out var all))
    {
        static int FindStartIndex(IReadOnlyList<Point> xs, int lastRowNo)
        {
            // xs is ordered by RowNo (ascending)
            int lo = 0;
            int hi = xs.Count - 1;
            int ans = xs.Count;
            while (lo <= hi)
            {
                int mid = lo + ((hi - lo) / 2);
                if (xs[mid].RowNo > lastRowNo)
                {
                    ans = mid;
                    hi = mid - 1;
                }
                else
                {
                    lo = mid + 1;
                }
            }
            return ans;
        }

        var start = FindStartIndex(all, c.LastRowNo);
        var slice = all.Skip(start).Take(pageSize).ToList();

        var done = slice.Count == 0 || (start + slice.Count) >= all.Count;
        var nextCursor2 = done ? null : Cursor.Encode(new Cursor(slice[^1].RowNo));

        AppendResultDump(resultId, c.LastRowNo, slice.Count, done, slice);

        var items2 = slice.Select(p => new { rowNo = p.RowNo, value = p.Value }).Cast<object>().ToList();
        return Results.Ok(new ResultResponse(items2, nextCursor2, done, slice.Count));
    }

    return Results.NotFound(new { message = "resultId not found" });
});

// health
app.MapGet("/api/transitiongraph/health", () => new { status = "OK" });

app.Lifetime.ApplicationStopping.Register(() =>
{
    // best-effort
    try
    {
        workers.ShutdownAsync().GetAwaiter().GetResult();
    }
    catch { /* ignore */ }
});

app.Run();