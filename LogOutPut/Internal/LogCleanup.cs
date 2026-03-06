using System.Globalization;
using System.Text.RegularExpressions;

namespace TransitionGraph.LogOutPut.Internal;

/// <summary>
/// LogCleanup-ログCleanup
/// 処理概略：LogCleanupに関する処理をまとめる
/// </summary>
internal static class LogCleanup
{
    // 対象:
    // TransitionGraph_YYYYMMDD.log
    // TransitionGraph_YYYYMMDD.log.1
    // TransitionGraph_YYYYMMDD.log.2 ...
    private static readonly Regex _regex =
        new(@"^TransitionGraph_(\d{8})\.log(\.\d+)?$", RegexOptions.IgnoreCase);

    /// <summary>
    /// DeleteOlderThanDays-削除OlderThanDays
    /// 処理概略：OlderThanDaysを削除する
    /// </summary>
    public static void DeleteOlderThanDays(string logDir, int keepDays)
    {
        if (!Directory.Exists(logDir)) return;

        foreach (var file in Directory.EnumerateFiles(logDir, "TransitionGraph_*.log*"))
        {
            var name = Path.GetFileName(file);
            var m = _regex.Match(name);
            if (!m.Success) continue;

            if (!DateTime.TryParseExact(m.Groups[1].Value, "yyyyMMdd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
            {
                continue;
            }

            if (dt < DateTime.Today.AddDays(-keepDays))
            {
                TryDelete(file);
            }
        }
    }

    /// <summary>
    /// TryDelete-試行削除
    /// 処理概略：Deleteを試行する
    /// </summary>
    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch
        {
            // ロック中などは無視（次回起動時に消える）
        }
    }
}