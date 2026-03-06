using System.Reflection;

namespace TransitionGraph.LogOutPut.Internal;

/// <summary>
/// PropertiesLoader-PropertiesLoader
/// 処理概略：Propertiesの読み込み(Loader)を行う
/// </summary>
internal static class PropertiesLoader
{
    /// <summary>
    /// GetPropertiesPath-取得PropertiesPath
    /// 処理概略：PropertiesPathを取得する
    /// </summary>
    public static string GetPropertiesPath()
    {
        // 呼び出し側のbinフォルダにコピーされたpropertiesを読む
        var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? ".";
        return Path.Combine(dir, "LogOutPut.properties");
    }

    /// <summary>
    /// Load-読込
    /// 処理概略：処理を読み込みする
    /// </summary>
    public static Dictionary<string, string> Load(string path)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!File.Exists(path))
        {
            // 無くても動く（デフォルト値で出力）
            return dict;
        }

        foreach (var raw in File.ReadAllLines(path))
        {
            var line = raw.Trim();
            if (line.Length == 0) continue;
            if (line.StartsWith("#")) continue;

            var idx = line.IndexOf('=');
            if (idx < 0) continue;

            var key = line[..idx].Trim();
            var val = line[(idx + 1)..].Trim();
            dict[key] = val;
        }

        return dict;
    }

    /// <summary>
    /// ParseInt-解析Int
    /// 処理概略：Intを解析する
    /// </summary>
    public static int ParseInt(string s, int defaultValue)
        => int.TryParse(s, out var v) ? v : defaultValue;
}