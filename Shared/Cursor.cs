using System.Text;
using System.Text.Json;

namespace TransitionGraph.Shared;

/// <summary>
/// Cursor-カーソル処理
/// 処理概略：カーソルに関する処理
/// </summary>
public sealed record Cursor(int LastRowNo)
{
    /// <summary>
    /// Encode-エンコード処理
    /// 処理概略：エンコードの処理する
    /// </summary>
    public static string Encode(Cursor cursor)
    {
        var json = JsonSerializer.Serialize(cursor);
        var bytes = Encoding.UTF8.GetBytes(json);
        return Base64UrlEncode(bytes);
    }

    /// <summary>
    /// Decode-デコード処理
    /// 処理概略：デコードを処理する
    /// </summary>
    public static Cursor Decode(string? cursor)
    {
        if (string.IsNullOrWhiteSpace(cursor))
            return new Cursor(0);

        var bytes = Base64UrlDecode(cursor);
        var json = Encoding.UTF8.GetString(bytes);
        return JsonSerializer.Deserialize<Cursor>(json) ?? new Cursor(0);
    }

    /// <summary>
    /// Base64UrlEncode-Base64UrlEncode
    /// 処理概略：Base64UrlEncodeを処理する
    /// </summary>
    private static string Base64UrlEncode(byte[] data)
    {
        return Convert.ToBase64String(data)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

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