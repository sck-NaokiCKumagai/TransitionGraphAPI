using System.Globalization;

namespace TransitionGraph.PortSetting;

/// <summary>
/// PropertiesFile-プロパティーファイル関連処理
/// 処理概略：プロパティーファイルに関する処理をまとめる
/// </summary>
internal static class PropertiesFile
{
    /// <summary>
    /// Load-読込
    /// 処理概略：処理を読み込みする
    /// </summary>
    public static Dictionary<string, string> Load(string filePath)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!File.Exists(filePath))
            return dict;

        foreach (var rawLine in File.ReadAllLines(filePath))
        {
            var line = rawLine.Trim();
            if (line.Length == 0) continue;
            if (line.StartsWith('#') || line.StartsWith(';')) continue;

            // Support key=value or key: value
            int idx = line.IndexOf('=');
            if (idx < 0) idx = line.IndexOf(':');
            if (idx <= 0) continue;

            var key = line.Substring(0, idx).Trim();
            var val = line.Substring(idx + 1).Trim();
            if (key.Length == 0) continue;

            dict[key] = val;
        }

        return dict;
    }

    /// <summary>
    /// GetInt-取得Int
    /// 処理概略：Intを取得する
    /// </summary>
    public static int GetInt(Dictionary<string, string> props, string key, int fallback)
    {
        if (!props.TryGetValue(key, out var s)) return fallback;
        if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return v;
        return fallback;
    }

    /// <summary>
    /// GetString-取得文字列
    /// 処理概略：Stringを取得する
    /// </summary>
    public static string GetString(Dictionary<string, string> props, string key, string fallback)
    {
        if (!props.TryGetValue(key, out var s)) return fallback;
        return string.IsNullOrWhiteSpace(s) ? fallback : s.Trim();
    }
}