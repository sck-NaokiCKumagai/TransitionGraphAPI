using System.Globalization;

namespace TransitionGraph.Shared;

/// <summary>
/// ResultStore-結果Store
/// 処理概略：Resultの保存/管理(Store)を行う
/// </summary>
public static class ResultStore
{
    // File format (template): CSV lines "RowNo,Value"
    // Stored under dataDir/{resultId}.csv
    /// <summary>
    /// GetPath-取得Path
    /// 処理概略：Pathを取得する
    /// </summary>
    public static string GetPath(string dataDir, string resultId)
        => Path.Combine(dataDir, $"{resultId}.csv");

    /// <summary>
    /// WriteAll-書込全体
    /// 処理概略：Allを書き込みする
    /// </summary>
    public static void WriteAll(string dataDir, string resultId, IEnumerable<(int RowNo, string Value)> items)
    {
        Directory.CreateDirectory(dataDir);
        var path = GetPath(dataDir, resultId);

        // Write atomically: temp -> move
        var tmp = path + ".tmp";
        using (var sw = new StreamWriter(File.Open(tmp, FileMode.Create, FileAccess.Write, FileShare.None)))
        {
            foreach (var (rowNo, value) in items)
            {
                // NOTE: This is a template; if your value can contain commas/newlines, switch to MessagePack/protobuf.
                sw.Write(rowNo.ToString(CultureInfo.InvariantCulture));
                sw.Write(',');
                sw.WriteLine(value.Replace("\r", " ").Replace("\n", " "));
            }
        }
        File.Move(tmp, path, overwrite: true);
    }

    /// <summary>
    /// ReadPage-読込Page
    /// 処理概略：Pageを読み取りする
    /// </summary>
    public static List<(int RowNo, string Value)> ReadPage(string dataDir, string resultId, int lastRowNo, int pageSize, out int nextLastRowNo, out bool done)
    {
        var path = GetPath(dataDir, resultId);
        if (!File.Exists(path))
        {
            nextLastRowNo = 0;
            done = true;
            return new List<(int, string)>();
        }

        var results = new List<(int, string)>(capacity: pageSize);
        int maxRow = lastRowNo;

        using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var sr = new StreamReader(fs);

        // Template simplification: scan sequentially until lastRowNo. For production, use DB paging or build an index.
        string? line;
        while ((line = sr.ReadLine()) is not null)
        {
            if (TryParseLine(line, out var rowNo, out var value))
            {
                if (rowNo <= lastRowNo) continue;
                results.Add((rowNo, value));
                maxRow = rowNo;
                if (results.Count >= pageSize) break;
            }
        }

        done = results.Count < pageSize; // if fewer than pageSize were found, we reached EOF
        nextLastRowNo = done ? 0 : maxRow;
        return results;
    }

    /// <summary>
    /// TryParseLine-試行解析Line
    /// 処理概略：ParseLineを試行する
    /// </summary>
    private static bool TryParseLine(string line, out int rowNo, out string value)
    {
        rowNo = 0;
        value = "";
        var idx = line.IndexOf(',');
        if (idx <= 0) return false;
        if (!int.TryParse(line[..idx], NumberStyles.Integer, CultureInfo.InvariantCulture, out rowNo)) return false;
        value = line[(idx + 1)..];
        return true;
    }
}