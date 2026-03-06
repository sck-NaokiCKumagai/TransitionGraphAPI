using System.Text.Json.Serialization;

namespace TransitionGraph.Dtos;

/// <summary>
/// Request result status (OK/NG/CANCEL/Running)
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RequestResultStatus
{
    Running,
    OK,
    NG,
    CANCEL
}

/// <summary>
/// Common request header shared by all requests.
///
/// Excel definition:
///  - GUID
///  - RequestId (1-9)
///  - SequenceNo
///  - parameter[10]
///  - RequestKey (Dictionary&lt;LotName, List&lt;ItemName&gt;&gt;)
///  - RequestResult
///
/// Note:
///  - Current REST API uses <see cref="RequestKey"/> as the numeric request id.
///  - <see cref="RequestId"/> is provided as an alias for readability.
/// </summary>
public sealed class RequestHeaderDto
{
    /// <summary>
    /// クライアントGUID
    /// (Excel: GUID)
    /// </summary>
    [JsonPropertyName("GUID")]
    public string GUID { get; set; } = string.Empty;

    /// <summary>
    /// リクエストID（1-9）
    /// (Excel: RequestId)
    /// </summary>
    public string RequestId { get; set; } = string.Empty;

    /// <summary>
    /// シーケンス番号（1..）最大8桁
    /// (Excel: SequenceNo)
    /// </summary>
    public string? SequenceNo { get; set; }

    /// <summary>
    /// パラメータ（10枠）
    /// (Excel: parameter[10])
    /// </summary>
    public List<string> Parameter { get; set; } = new();

    /// <summary>
    /// リクエストキー（ロット名と項目名のディクショナリ）
    /// (Excel: reqKey.RequestKey)
    /// </summary>
    public Dictionary<string, List<string>> RequestKey { get; set; } = new();

    /// <summary>
    /// リクエスト結果（OK/NG/CANCEL/Running）
    /// (Excel: RequestResult)
    /// </summary>
    public RequestResultStatus RequestResult { get; set; } = RequestResultStatus.Running;

    /// <summary>
    /// Optional: identifies one client request instance (GUID string). Used for cancel.
    /// </summary>
    public string? RequestInstanceId { get; set; }

    // ------------------------
    // Backward compatible alias
    // ------------------------

    [JsonIgnore]
    public string ClientGuid
    {
        get => GUID;
        set => GUID = value;
    }

    [JsonIgnore]
    public string RequestKeyLegacy
    {
        get => RequestId;
        set => RequestId = value;
    }

    [JsonIgnore]
    public List<string> Parameters
    {
        get => Parameter;
        set => Parameter = value;
    }

    [JsonIgnore]
    public Dictionary<string, List<string>> RequestKeyMap
    {
        get => RequestKey;
        set => RequestKey = value;
    }
}

// =========================
// REST request/response DTOs (Spec (4))
// =========================

/// <summary>
/// Spec (4-1): Request is only RequestHeader.
/// </summary>
public sealed class RequestDto
{
    public RequestHeaderDto Header { get; set; } = new();
}

/// <summary>
/// Generic response envelope: Header + Answer.
/// Spec (4-2)
/// </summary>
public sealed class ResponseDto<TAnswer>
{
    public RequestHeaderDto Header { get; set; } = new();
    public TAnswer Answer { get; set; } = default!;
}

/// <summary>
/// Spec (4-2): Request answer wrapper.
///
/// NOTE:
/// The Excel/spec uses the name "RequestAnwer" (typo).
/// This DTO is kept with the same name to stay compatible with existing code.
/// </summary>
public sealed class RequestAnwerDto
{
    /// <summary>
    /// リクエストアンサー（内容はリクエストIDにより異なる）
    /// </summary>
    public object? RequestAnswer { get; set; }
}

public sealed class QueryAnswerDto
{
    public string ResultId { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int PageSize { get; set; }
    public string NextCursor { get; set; } = string.Empty;
}

public sealed record ResultResponse(List<object> Items, string? NextCursor, bool Done, int ReturnedCount);

public sealed class CancelRequestDto
{
    public RequestHeaderDto Header { get; set; } = new();
    public string? ResultId { get; set; }
}

public sealed class CancelAnswerDto
{
    public bool Canceled { get; set; }
    public string Message { get; set; } = string.Empty;
}

// =========================
// RequestId specific answer DTOs
// =========================

/// <summary>RequestId=1 工程取得</summary>
public sealed class WorkProcessDto
{
    public List<string> ProcessName { get; set; } = new();
}

/// <summary>RequestId=2 ロット＆項目名取得</summary>
public sealed class LotItemNameDto
{
    public Dictionary<string, List<string>> Lot_Item_Name { get; set; } = new();
}

/// <summary>Lot info structure used by RequestId=4 and 7.</summary>
public sealed class LotInfoDto
{
    public string LotName { get; set; } = string.Empty;
    public string ProductInfo { get; set; } = string.Empty;
    public int WaferNo { get; set; }
    public string WaferId { get; set; } = string.Empty;
    public string BaseProcess { get; set; } = string.Empty;
    public string Work { get; set; } = string.Empty;
}

/// <summary>RequestId=4 ロット情報取得</summary>
public sealed class LotInformationDto
{
    public List<LotInfoDto> LotInfo { get; set; } = new();
}

/// <summary>統計情報</summary>
public sealed class StatisticsInfoDto
{
    public double Minimum { get; set; }
    public double Maximum { get; set; }
    public double Average { get; set; }
    public double Median { get; set; }
    public int DataCount { get; set; }
    public double Sigma { get; set; }
    public double IQR { get; set; }

    public double Min_Average { get; set; }
    public double Max_Average { get; set; }
    public double Min_Median { get; set; }
    public double Max_Median { get; set; }

    public double PlsSPECMainLot { get; set; }
    public double MisSPECMainLot { get; set; }
    public double PlsLIMITMainLot { get; set; }
    public double MisLIMITMainLot { get; set; }
}

/// <summary>
/// Graph point data used by RequestId=5.
/// Each point corresponds to one X element and its statistics.
/// </summary>
public sealed class GraphPointDataDto
{
    public string Xelement { get; set; } = string.Empty;
    public StatisticsInfoDto StatisticsInfo { get; set; } = new();
}

// =========================
// RequestId=5 (Revised) DTOs
// =========================

/// <summary>
/// RequestId=5 Graph point data (Lot/Wafer) - revised spec.
/// </summary>
public sealed class GraphPointData5Dto
{
    public double Minimum { get; set; }
    public double Maximum { get; set; }
    public double Average { get; set; }
    public double Median { get; set; }
    public int DataCount { get; set; }
    public double Sigma { get; set; }
    public double IQR { get; set; }

    public List<string> HistoryInfo { get; set; } = new();
}

/// <summary>
/// RequestId=5 GraphData - revised spec.
/// </summary>
public sealed class GraphData5Dto
{
    public string LotName { get; set; } = string.Empty;
    public string ItemName { get; set; } = string.Empty;

    /// <summary>データ粒度 ("Lot" / "Wafer")</summary>
    public string DataSort { get; set; } = string.Empty;

    /// <summary>データ(ロット)</summary>
    public GraphPointData5Dto? GraphPointData { get; set; }

    /// <summary>
    /// データ(ウェーハ)
    ///  - key: WaferID (WAFER_NO)
    ///  - value: wafer point data (null when skipped)
    /// </summary>
    public Dictionary<string, GraphPointData5Dto?>? WaferGraphPointData { get; set; }

    /// <summary>統計情報</summary>
    public StatisticsInfoDto StatisticsInfo { get; set; } = new();
}

// =========================
// RequestId=6 (Revised) DTOs
// =========================

/// <summary>
/// RequestId=6 Histogram point data (Lot/Wafer) - revised spec.
/// </summary>
public sealed class HistgramPointData6Dto
{
    public double Minimum { get; set; }
    public double Maximum { get; set; }
    public double Average { get; set; }
    public double Median { get; set; }
    public int DataCount { get; set; }
    public double Sigma { get; set; }
    public double IQR { get; set; }

    /// <summary>Chip data (RESULT list).</summary>
    public List<float> ChipData { get; set; } = new();

    /// <summary>
    /// History info (EQUIPMENT_ID / PROGRAM etc.).
    /// Spec text references this field even though the class list omits it.
    /// </summary>
    public List<string> HistoryInfo { get; set; } = new();
}

/// <summary>
/// RequestId=6 Histogram graph data (revised spec).
/// </summary>
public sealed class HistGraphData6Dto
{
    public string LotName { get; set; } = string.Empty;
    public string ItemName { get; set; } = string.Empty;

    /// <summary>データ粒度 ("Lot" / "Wafer")</summary>
    public string DataSort { get; set; } = string.Empty;

    /// <summary>データ(ロット) - null when DataSort=Wafer</summary>
    public HistgramPointData6Dto? HistgramPointData { get; set; }

    /// <summary>
    /// データ(ウェーハ)
    /// - key: WaferID (WAFER_NO)
    /// - value: wafer histogram point data (null when skipped)
    ///
    /// NOTE: The spec names this property "Dictionary".
    /// </summary>
    public Dictionary<string, HistgramPointData6Dto?>? Dictionary { get; set; }

    /// <summary>統計情報</summary>
    public StatisticsInfoDto StaticticsInfo { get; set; } = new();
}

/// <summary>
/// Histogram point data used by RequestId=6.
/// </summary>
public sealed class HistgramPointDataDto
{
    public string Xelement { get; set; } = string.Empty;
    public StatisticsInfoDto StatisticsInfo { get; set; } = new();

    /// <summary>Histogram data array.</summary>
    public List<double> Data { get; set; } = new();

    /// <summary>End flag for continuation.</summary>
    public bool EndFlag { get; set; }
}

/// <summary>RequestId=6 Histogram GraphData response container (Spec name: HistGraphDataDto)</summary>
public sealed class HistGraphDataDto
{
    /// <summary>項目名</summary>
    public string ItemName { get; set; } = string.Empty;

    /// <summary>全体統計情報</summary>
    public AllStatsDto AllStats { get; set; } = new();

    /// <summary>ロット情報</summary>
    public List<LotDto> Lots { get; set; } = new();
}

/// <summary>RequestId=5/6 GraphData response container</summary>
public sealed class GraphDataDto
{
    /// <summary>項目名</summary>
    public string ItemName { get; set; } = string.Empty;

    /// <summary>全体統計情報</summary>
    public AllStatsDto AllStats { get; set; } = new();

    /// <summary>ロット情報</summary>
    public List<LotDto> Lots { get; set; } = new();
}

public sealed class AllStatsDto
{
    public double Maximum { get; set; }
    public double Minimum { get; set; }
    public double Average { get; set; }
    public double Median { get; set; }
    public int DataCount { get; set; }
    public double Sigma { get; set; }
    public double IQR { get; set; }

    public double Min_Average { get; set; }
    public double Max_Average { get; set; }
    public double Min_Median { get; set; }
    public double Max_Median { get; set; }

    public double PlsSPEC { get; set; }
    public double MisSPEC { get; set; }
    public double PlsLimit { get; set; }
    public double MisLimit { get; set; }

    // RequestId=8: Spec/Limit values for representative (main) lot.
    // When the value cannot be retrieved, it should be null.
    public double? PlsSPECMainLot { get; set; }
    public double? MisSPECMainLot { get; set; }
    public double? PlsLIMITMainLot { get; set; }
    public double? MisLIMITMainLot { get; set; }
}

public sealed class LotDto
{
    /// <summary>ロット名</summary>
    public string LotName { get; set; } = string.Empty;

    /// <summary>ロット統計情報</summary>
    public StatsDto Stats { get; set; } = new();

    /// <summary>ウェーハ情報（DataSort=Lot の場合は空）</summary>
    public List<WaferDto> Wafers { get; set; } = new();

    /// <summary>
    /// ウェーハ情報（RequestId=9 用）
    /// NOTE: 既存の RequestId=5/6/8 のレスポンス互換性を維持するため、追加プロパティとして定義する。
    /// </summary>
    [JsonPropertyName("waferInChipDto")]
    public List<WaferInChipDto> WaferInChipDto { get; set; } = new();
}

/// <summary>
/// RequestId=9: Wafer information with Chip list.
/// </summary>
public sealed class WaferInChipDto
{
    /// <summary>ウェーハID</summary>
    public string WaferId { get; set; } = string.Empty;

    /// <summary>ウェーハ統計情報（チップデータから算出）</summary>
    public StatsDto Stats { get; set; } = new();

    /// <summary>チップ情報（RequestId=9 用）</summary>
    [JsonPropertyName("chipDto")]
    public List<Chip9Dto> ChipDto { get; set; } = new();
}

/// <summary>
/// RequestId=9: chip data (ID + value).
/// Note: Name is suffixed to avoid breaking existing <see cref="TransitionGraph.Dtos.ChipDto"/> used by RequestId=6.
/// </summary>
public sealed class Chip9Dto
{
    public string ChipId { get; set; } = string.Empty;
    // Nullable to allow "missing" chip values to be represented as empty (null) in JSON.
    public double? Data { get; set; }
}

public sealed class WaferDto
{
    /// <summary>ウェーハID（WAFER_STATISTIC_DATA.WAFER_ID）</summary>
    public string WaferId { get; set; } = string.Empty;

    public StatsDto Stats { get; set; } = new();

    /// <summary>チップ情報（RequestId=6 用）</summary>
    public List<ChipDto> Chips { get; set; } = new();
}



public sealed class ChipDto
{
    public int ChipId { get; set; }

    public StatsDto Stats { get; set; } = new();
}
public sealed class StatsDto
{
    public double Maximum { get; set; }
    public double Minimum { get; set; }
    public double Average { get; set; }
    public double Median { get; set; }
    public int DataCount { get; set; }
    public double Sigma { get; set; }
    public double IQR { get; set; }

    public List<string> HistoryInfo { get; set; } = new();
}

/// <summary>RequestId=7 ISTAR連携（グリッドデータ付随情報）取得</summary>
public sealed class ISTARInfoDto
{
    public List<string> LotNameList { get; set; } = new();
    public List<string> ItemNameList { get; set; } = new();
    public List<string> CateNameList { get; set; } = new();
    public string FilterInfo { get; set; } = string.Empty;
    public List<LotInfo> LotInfoList { get; set; } = new();
}

public sealed class LotInfo
{
    public string LotName { get; set; } = string.Empty;
    public string ProductInfo { get; set; } = string.Empty;
    public int WaferNo { get; set; }
    public string WaferId { get; set; } = string.Empty;
    public string StanderdProc { get; set; } = string.Empty;
    // JSON field name must be "process" per spec.
    [System.Text.Json.Serialization.JsonPropertyName("process")]
    public string Process { get; set; } = string.Empty;
}

/// <summary>RequestId=8 ISTAR連携(未定義) placeholder</summary>
public sealed class IstarRequest8Dto
{
}

/// <summary>RequestId=9 ISTAR連携(未定義) placeholder</summary>
public sealed class IstarRequest9Dto
{
}

