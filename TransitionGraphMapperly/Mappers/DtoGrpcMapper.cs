using Riok.Mapperly.Abstractions;
using System.Globalization;
using TransitionGraph.Dtos;
using TransitionGraph.V1;

namespace TransitionGraphAPI.Mappers;

/// <summary>
/// Mapperly based mappers for DTO <-> gRPC message conversion.
///
/// (12)戻り値データのセット高速化のため、手書き代入を減らし
/// ソースジェネレータでマッピングコードを生成する。
/// </summary>
[Mapper]
public static partial class DtoGrpcMapper
{
    // -----------------
    // Header
    // -----------------
    // NOTE:
    // - DTO側とgRPC側でプロパティ名の大小文字が異なるため、明示的に変換する。
    // - RequestResultStatus は DTO と gRPC で同名 enum があるので手動変換にする。

    public static RequestHeader ToGrpc(RequestHeaderDto src)
        => new()
        {
            Guid = src.GUID ?? string.Empty,
            RequestId = src.RequestId ?? string.Empty,
            SequenceNo = src.SequenceNo ?? string.Empty,
            RequestInstanceId = src.RequestInstanceId ?? string.Empty,
            RequestResult = ToGrpc(src.RequestResult)
        };

    public static RequestHeaderDto ToDto(RequestHeader src)
        => new()
        {
            GUID = src.Guid ?? string.Empty,
            RequestId = src.RequestId ?? string.Empty,
            SequenceNo = src.SequenceNo ?? string.Empty,
            RequestInstanceId = string.IsNullOrWhiteSpace(src.RequestInstanceId) ? null : src.RequestInstanceId,
            RequestResult = ToDto(src.RequestResult)
        };

    private static TransitionGraph.V1.RequestResultStatus ToGrpc(TransitionGraph.Dtos.RequestResultStatus src)
        => src switch
        {
            TransitionGraph.Dtos.RequestResultStatus.OK => TransitionGraph.V1.RequestResultStatus.Ok,
            TransitionGraph.Dtos.RequestResultStatus.NG => TransitionGraph.V1.RequestResultStatus.Ng,
            TransitionGraph.Dtos.RequestResultStatus.CANCEL => TransitionGraph.V1.RequestResultStatus.Cancel,
            _ => TransitionGraph.V1.RequestResultStatus.Running,
        };

    private static TransitionGraph.Dtos.RequestResultStatus ToDto(TransitionGraph.V1.RequestResultStatus src)
        => src switch
        {
            TransitionGraph.V1.RequestResultStatus.Ok => TransitionGraph.Dtos.RequestResultStatus.OK,
            TransitionGraph.V1.RequestResultStatus.Ng => TransitionGraph.Dtos.RequestResultStatus.NG,
            TransitionGraph.V1.RequestResultStatus.Cancel => TransitionGraph.Dtos.RequestResultStatus.CANCEL,
            _ => TransitionGraph.Dtos.RequestResultStatus.Running,
        };

    /// <summary>
    /// RESTのRequestHeaderDtoから BuildRequest を生成する。
    /// - parameter[10] は gRPC parameters(Parm1..Parm10) に変換
    /// - RequestKey(Dictionary) は gRPC header.requestKey(map) に変換
    /// </summary>
    public static BuildRequest ToGrpcBuildRequest(RequestHeaderDto header)
    {
        var grpc = new BuildRequest
        {
            Header = ToGrpc(header),
            SqlKey = header.RequestId ?? string.Empty,
            TotalCount = 0,
        };

        // parameter list -> parameters
        // NOTE:
        // - For most requests we keep the legacy Parm1..ParmN mapping.
        // - For some RequestIds, DataEdit expects named keys (e.g. process/startTime/endTime, lotNamesJson).
        if (header.Parameter is not null)
        {
            var rid = header.RequestId ?? string.Empty;

            // RequestId=4: parameter[0]=process, parameter[1..]=lot name list
            // Keep the screen flow as List<string>; transport it as JSON array (no CSV).
            if (string.Equals(rid, "4", StringComparison.OrdinalIgnoreCase))
            {
                var proc = header.Parameter.ElementAtOrDefault(0) ?? string.Empty;
                var lots = header.Parameter.Skip(1).Where(x => !string.IsNullOrWhiteSpace(x)).ToList();

                grpc.Parameters["process"] = proc;
                grpc.Parameters["lotNamesJson"] = System.Text.Json.JsonSerializer.Serialize(lots);

                // Backward compatibility: keep Parm1 only (process).
                grpc.Parameters["Parm1"] = proc;
            }
            else
            {
                // Default: Parm1..ParmN mapping
                for (var i = 0; i < header.Parameter.Count; i++)
                {
                    grpc.Parameters[$"Parm{i + 1}"] = header.Parameter[i] ?? string.Empty;
                }

                // RequestId=2: parameter[0]=process, [1]=startTime, [2]=endTime
                // DataEdit expects these named keys.
                if (string.Equals(rid, "2", StringComparison.OrdinalIgnoreCase))
                {
                    grpc.Parameters["process"] = header.Parameter.ElementAtOrDefault(0) ?? string.Empty;
                    grpc.Parameters["startTime"] = header.Parameter.ElementAtOrDefault(1) ?? string.Empty;
                    grpc.Parameters["endTime"] = header.Parameter.ElementAtOrDefault(2) ?? string.Empty;
                }
            }
        }

        // RequestKey map: lot -> item list
        if (header.RequestKey is not null)
        {
            foreach (var kv in header.RequestKey)
            {
                var v = new RequestKeyValue();
                if (kv.Value is not null)
                    v.ItemName.AddRange(kv.Value);
                grpc.Header.RequestKey[kv.Key] = v;
            }
        }

        return grpc;
    }

    // -----------------
    // /cancel
    // -----------------
    public static CancelRequest ToGrpc(CancelRequestDto src)
        => new()
        {
            Header = src.Header is null ? new RequestHeader() : ToGrpc(src.Header),
            ResultId = src.ResultId ?? string.Empty,
        };

    public static CancelAnswerDto ToDto(CancelReply src)
        => new()
        {
            Canceled = src.Canceled,
            Message = src.Message ?? string.Empty,
        };
}
