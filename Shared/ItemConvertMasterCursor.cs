using System.Text;
using System.Text.Json;

namespace TransitionGraph.Shared;

/// <summary>
/// Cursor for paging ITEM_CONVERT_MASTER in stable order (EQUIPMENTID, CONVERTORDER).
/// ResultId is a correlation id for the fetch session (not used for SQL itself).
/// </summary>
public sealed record ItemConvertMasterCursor(string ResultId, string LastEquipmentId, long LastConvertOrder)
{
    /// <summary>
    /// Encode-エンコード処理
    /// 処理概略：エンコードを処理する
    /// </summary>
    public static string Encode(ItemConvertMasterCursor cursor)
    {
        var json = JsonSerializer.Serialize(cursor);
        var bytes = Encoding.UTF8.GetBytes(json);
        return Base64UrlEncode(bytes);
    }

    /// <summary>
    /// Decode-デコード処理
    /// 処理概略：デコード処理する
    /// </summary>
    public static ItemConvertMasterCursor Decode(string? cursor)
    {
        if (string.IsNullOrWhiteSpace(cursor))
            return new ItemConvertMasterCursor("", "", 0);

        var bytes = Base64UrlDecode(cursor);
        var json = Encoding.UTF8.GetString(bytes);
        return JsonSerializer.Deserialize<ItemConvertMasterCursor>(json) ?? new ItemConvertMasterCursor("", "", 0);
    }

    /// <summary>
    /// Base64UrlEncode-Base64UrlEncode
    /// 処理概略：Base64UrlEncodeを処理する
    /// </summary>
    private static string Base64UrlEncode(byte[] data)
        => Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /// <summary>
    /// Base64UrlDecode-Base64UrlDecode
    /// 処理概略：Base64UrlDecodeを処理する
    /// </summary>
    private static byte[] Base64UrlDecode(string base64Url)
    {
        var s = base64Url.Replace('-', '+').Replace('_', '/');
        switch (s.Length % 4)
        {
            case 2: s += "=="; break;
            case 3: s += "="; break;
        }
        return Convert.FromBase64String(s);
    }
}