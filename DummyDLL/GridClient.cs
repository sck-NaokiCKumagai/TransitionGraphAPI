using System.Globalization;

namespace DXC.EngPF.Core.Grid;

/// <summary>
/// Dummy implementation of DXC.EngPF.Core.Grid.dll.
///
/// This implementation reads simple CSV files and returns results that mimic
/// the minimal surface needed by RequestId=7.
///
/// CSV format (simple, flexible):
/// - 1st row: column definitions, comma separated.
///   Each cell may be either:
///     - ColumnId
///     - ColumnId|DataItemKind
///     - ColumnId|DataItemKind|Flags
///   Flags (optional, case-insensitive, semicolon separated):
///     - header   : marks this column as header (IsHeader=true)
///     - numeric  : marks this column as numeric (IsNnumeric=true)
///
/// - Remaining rows: data values (strings). Empty cell => null.
/// Notes:
/// - This parser is intentionally simple; quoting is not supported.
/// - For demo use, keep values free of commas/newlines.
/// </summary>
public sealed class GridClient
{
    private readonly string _csvDirectory;

    // Extra fallback roots (resolved at runtime)
    private readonly List<string> _fallbackDirectories;

    /// <summary>
    /// GridClient-Gridクライアント
    /// 処理概略：GridClientの初期化を行う
    /// </summary>
    public GridClient(string csvDirectory)
    {
        _csvDirectory = csvDirectory;

        _fallbackDirectories = new List<string>();

        // 1) Configured directory (as-is)
        TryAddDir(_fallbackDirectories, _csvDirectory);

        // 2) If the configured directory is something like "DummyDLL/CSV" resolved
        // under another base dir, also try its "CSV" child just in case.
        TryAddDir(_fallbackDirectories, Path.Combine(_csvDirectory, "CSV"));

        // 3) Assembly-relative fallbacks (covers cases where CSV is placed under DummyDLL/bin/.../CSV)
        try
        {
            var asmDir = Path.GetDirectoryName(typeof(GridClient).Assembly.Location) ?? string.Empty;
            TryAddDir(_fallbackDirectories, Path.Combine(asmDir, "CSV"));
            TryAddDir(_fallbackDirectories, Path.Combine(asmDir, "..", "CSV"));
            TryAddDir(_fallbackDirectories, Path.Combine(asmDir, "..", "..", "CSV"));
            TryAddDir(_fallbackDirectories, Path.Combine(asmDir, "..", "..", "..", "CSV"));
        }
        catch
        {
            // ignore
        }

        // 4) Repo-relative fallbacks (covers "DummyDLL/bin/.../CSV" when running DataEdit from its own bin)
        // Walk up a few levels from the current AppContext.BaseDirectory and add common DummyDLL output locations.
        try
        {
            var cur = AppContext.BaseDirectory;
            for (int i = 0; i < 6; i++)
            {
                var parent = Path.GetDirectoryName(cur);
                if (string.IsNullOrWhiteSpace(parent)) break;
                cur = parent;

                TryAddDir(_fallbackDirectories, Path.Combine(cur, "DummyDLL", "CSV"));
                TryAddDir(_fallbackDirectories, Path.Combine(cur, "DummyDLL", "bin", "Debug", "net8.0", "CSV"));
                TryAddDir(_fallbackDirectories, Path.Combine(cur, "DummyDLL", "bin", "Release", "net8.0", "CSV"));
            }
        }
        catch
        {
            // ignore
        }
    }

    /// <summary>
    /// GetColumnDataAndRowCountAsync-取得列データAnd行Count（非同期）
    /// 処理概略：ColumnDataAndRowCountAsyncを取得する
    /// </summary>
    public Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, CancellationToken cancellationToken = default)
    {
        var table = LoadTable(secondCacheId);
        var result = new ColumnDataAndRowCountResult
        {
            RowCount = table.RowCount,
            ColumnDataArray = table.Columns
        };
        return Task.FromResult(result);
    }

    /// <summary>
    /// GetGridDataAsync-取得Gridデータ（非同期）
    /// 処理概略：GridDataAsyncを取得する
    /// </summary>
    public Task<GridDataResult> GetGridDataAsync(
        string[] columnIds,
        string gridId,
        bool isGetVirtualColumnValue,
        int height,
        int width,
        int x,
        int y,
        CancellationToken cancellationToken = default)
    {
        var table = LoadTable(gridId);

        // Return column-major data: GridData[colIndex][rowIndex]
        var colMajor = new List<ICollection<object?>>(capacity: table.Columns.Count);
        for (int ci = 0; ci < table.Columns.Count; ci++)
        {
            var col = new List<object?>(capacity: table.RowCount);
            for (int ri = 0; ri < table.RowCount; ri++)
                col.Add(table.Data[ri][ci]);
            colMajor.Add(col);
        }

        return Task.FromResult(new GridDataResult { GridData = colMajor });
    }

    /// <summary>
    /// GetGridFilterInfoAsync-取得GridFilterInfo（非同期）
    /// 処理概略：GridFilterInfoAsyncを取得する
    /// </summary>
    public Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, CancellationToken cancellationToken = default)
    {
        // Optional filter file:
        // - {secondCacheId}.filter.csv  (dot style)
        // - {secondCacheId}_filter.csv  (underscore style)
        // Format: ColumnName,FilterString,IsCurrentFilter
        var path = FindExistingFile(
            $"{secondCacheId}.filter.csv",
            $"{secondCacheId}_filter.csv");
        var list = new List<FilterInfoString>();

        if (File.Exists(path))
        {
            foreach (var line in File.ReadAllLines(path))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                // Allow header row
                if (line.TrimStart().StartsWith("ColumnName", StringComparison.OrdinalIgnoreCase))
                    continue;

                var parts = line.Split(',', 3);
                if (parts.Length < 2) continue;

                var col = parts[0].Trim();
                var filter = parts[1].Trim();
                var isCurrent = true;
                if (parts.Length >= 3)
                {
                    _ = bool.TryParse(parts[2].Trim(), out isCurrent);
                }

                list.Add(new FilterInfoString
                {
                    ColumnName = col,
                    FilterString = filter,
                    IsCurrentFilter = isCurrent
                });
            }
        }

        return Task.FromResult(new GridFilterInfoResult { FilterInfoList = list });
    }

    /// <summary>
    /// RequestId=10: Set filter and sort state.
    ///
    /// The real DXC.EngPF.Core.Grid.dll performs in-memory state changes.
    /// In this dummy DLL, we accept the DTO and return the fixed string
    /// required by the spec.
    /// </summary>
    public Task<string> SetFilterAndSortAsync(FilterAndSortParam param, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // No-op in dummy.
        return Task.FromResult("Accept Request");
    }

    // =========================
    // Internal table loader
    // =========================

    
    /// <summary>
    /// LooksLikeFlags-LooksLikeFlags
    /// 処理概略：LooksLikeFlagsを処理する
    /// </summary>
    private static bool LooksLikeFlags(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return false;
        var t = s.Trim();
        // Flags are semicolon-separated tokens such as "header" and/or "numeric".
        // We keep this heuristic strict so that history column ids like "...|装置ID" are NOT treated as flags.
        return t.Contains("header", StringComparison.OrdinalIgnoreCase)
               || t.Contains("numeric", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// LooksLikeKind-LooksLikeKind
    /// 処理概略：LooksLikeKindを処理する
    /// </summary>
    private static bool LooksLikeKind(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return false;
        var t = s.Trim();
        // Known kinds used by DTO extraction (RequestId=7)
        return t.Equals("CATEGORY", StringComparison.OrdinalIgnoreCase)
               || t.Equals("WAFERMAP", StringComparison.OrdinalIgnoreCase)
               || t.Equals("SLOT", StringComparison.OrdinalIgnoreCase)
               || t.Equals("SITE", StringComparison.OrdinalIgnoreCase);
    }

	/// <summary>
	/// LoadTable-読込Table
	/// 処理概略：Tableを読み込みする
	/// </summary>
	private LoadedTable LoadTable(string secondCacheId)
    {
        // Data file: {secondCacheId}.csv
        var path = FindExistingFile($"{secondCacheId}.csv");
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return new LoadedTable(new List<DataItemColumnData>(), new List<List<object?>>());

        var lines = File.ReadAllLines(path)
            .Where(l => !string.IsNullOrWhiteSpace(l))
            .ToList();

        if (lines.Count == 0) return new LoadedTable(new List<DataItemColumnData>(), new List<List<object?>>());

        var headerCells = lines[0].Split(',').Select(c => c.Trim()).ToList();
        var columns = BuildColumns(secondCacheId, headerCells);

        // Data rows
        var data = new List<List<object?>>(capacity: Math.Max(0, lines.Count - 1));
        for (int i = 1; i < lines.Count; i++)
        {
            var cells = lines[i].Split(',');
            var row = new List<object?>(capacity: columns.Count);
            for (int c = 0; c < columns.Count; c++)
            {
                var raw = c < cells.Length ? cells[c].Trim() : string.Empty;
                row.Add(string.IsNullOrWhiteSpace(raw) ? null : raw);
            }
            data.Add(row);
        }

        // Infer numeric columns if not explicitly flagged
        InferNumericFlags(columns, data);

        return new LoadedTable(columns, data);
    }

    /// <summary>
    /// TryAddDir-試行AddDir
    /// 処理概略：AddDirを試行する
    /// </summary>
    private static void TryAddDir(List<string> list, string? dir)
    {
        if (string.IsNullOrWhiteSpace(dir)) return;
        try
        {
            var full = Path.GetFullPath(dir);
            if (!list.Contains(full, StringComparer.OrdinalIgnoreCase))
                list.Add(full);
        }
        catch
        {
            // ignore
        }
    }

    /// <summary>
    /// FindExistingFile-検索ExistingFile
    /// 処理概略：ExistingFileを検索する
    /// </summary>
    private string FindExistingFile(params string[] fileNames)
    {
        foreach (var root in _fallbackDirectories)
        {
            foreach (var fn in fileNames)
            {
                var p = Path.Combine(root, fn);
                if (File.Exists(p)) return p;
            }
        }

        // last resort: also try current directory
        foreach (var fn in fileNames)
        {
            var p = Path.GetFullPath(fn);
            if (File.Exists(p)) return p;
        }

        // default to first candidate under configured dir for compatibility
        return Path.Combine(_csvDirectory, fileNames.FirstOrDefault() ?? string.Empty);
    }

    /// <summary>
    /// BuildColumns-生成列
    /// 処理概略：Columnsを生成する
    /// </summary>
    private List<DataItemColumnData> BuildColumns(string secondCacheId, List<string> headerCells)
    {
        // If header uses "ColumnId|Kind|Flags" style, honor it.
        var hasPipe = headerCells.Any(c => c.Contains('|'));
        if (hasPipe)
        {
            var cols = new List<DataItemColumnData>(capacity: headerCells.Count);
            foreach (var cell in headerCells)
            {
                var parts = cell.Split('|', StringSplitOptions.TrimEntries);

                // NOTE:
                // ColumnId itself may contain '|' (e.g. 履歴測定|...|装置ID).
                // In that case we must preserve the full ColumnId and *only* treat the tail segments
                // as metadata when they look like (DataItemKind and/or Flags).
                string colId = cell;
                string kind = string.Empty;
                string flags = string.Empty;

                if (parts.Length >= 3 && LooksLikeKind(parts[^2]) && LooksLikeFlags(parts[^1]))
                {
                    colId = string.Join("|", parts.Take(parts.Length - 2));
                    kind = parts[^2];
                    flags = parts[^1];
                }
                else if (parts.Length >= 2 && LooksLikeKind(parts[^1]) && !LooksLikeFlags(parts[^1]))
                {
                    colId = string.Join("|", parts.Take(parts.Length - 1));
                    kind = parts[^1];
                }

                var isHeader = false;
                var isNumeric = false;
                if (!string.IsNullOrWhiteSpace(flags))
                {
                    foreach (var f in flags.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
                    {
                        if (string.Equals(f, "header", StringComparison.OrdinalIgnoreCase)) isHeader = true;
                        if (string.Equals(f, "numeric", StringComparison.OrdinalIgnoreCase)) isNumeric = true;
                    }
                }

                cols.Add(new DataItemColumnData
                {
                    ColumnId = colId,
                    DataItemKind = kind,
                    IsHeader = isHeader,
                    IsNnumeric = isNumeric,
                });
            }
            return cols;
        }

        // Otherwise, look for a separate columns metadata CSV:
        // - {id}.columns.csv (dot style)
        // - {id}_columns.csv (underscore style)
        var metaPath = FindExistingFile(
            $"{secondCacheId}.columns.csv",
            $"{secondCacheId}_columns.csv");

        if (File.Exists(metaPath))
        {
            // Expected format (with header row):
            // ColumnId,IsHeader,IsNnumeric,DataItemKind
            var map = new Dictionary<string, DataItemColumnData>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadAllLines(metaPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.TrimStart().StartsWith("ColumnId", StringComparison.OrdinalIgnoreCase))
                    continue;

                var parts = line.Split(',', 4);
                if (parts.Length < 1) continue;
                var colId = parts[0].Trim();
                if (string.IsNullOrWhiteSpace(colId)) continue;

                bool isHeader = false;
                bool isNumeric = false;
                string kind = string.Empty;

                if (parts.Length >= 2) _ = bool.TryParse(parts[1].Trim(), out isHeader);
                if (parts.Length >= 3) _ = bool.TryParse(parts[2].Trim(), out isNumeric);
                if (parts.Length >= 4) kind = parts[3].Trim();

                map[colId] = new DataItemColumnData
                {
                    ColumnId = colId,
                    DataItemKind = kind,
                    IsHeader = isHeader,
                    IsNnumeric = isNumeric,
                };
            }

            // Preserve header order
            var cols = new List<DataItemColumnData>(headerCells.Count);
            foreach (var h in headerCells)
            {
                if (map.TryGetValue(h, out var c))
                {
                    cols.Add(c);
                }
                else
                {
                    cols.Add(new DataItemColumnData { ColumnId = h });
                }
            }
            return cols;
        }

        // Fallback: minimal columns (no kinds/flags)
        return headerCells.Select(h => new DataItemColumnData { ColumnId = h }).ToList();
    }

    /// <summary>
    /// InferNumericFlags-InferNumericFlags
    /// 処理概略：InferNumericFlagsを処理する
    /// </summary>
    private static void InferNumericFlags(List<DataItemColumnData> columns, List<List<object?>> data)
    {
        for (int c = 0; c < columns.Count; c++)
        {
            if (columns[c].IsNnumeric) continue;
            bool allNumeric = true;
            foreach (var row in data)
            {
                var v = row[c]?.ToString();
                if (string.IsNullOrWhiteSpace(v)) continue;
                if (!double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
                {
                    allNumeric = false;
                    break;
                }
            }
            columns[c].IsNnumeric = allNumeric;
        }
    }

    /// <summary>
    /// LoadedTable-LoadedTable
    /// 処理概略：LoadedTableに関する処理をまとめる
    /// </summary>
    private sealed record LoadedTable(List<DataItemColumnData> Columns, List<List<object?>> Data)
    {
        public int RowCount => Data.Count;
    }
}

// =========================
// Dummy DTOs mirroring the needed surface
// =========================

/// <summary>
/// ColumnDataAndRowCountResult-列データAnd行Count結果
/// 処理概略：ColumnDataAndRowCountResultに関する処理をまとめる
/// </summary>
public sealed class ColumnDataAndRowCountResult
{
    public int RowCount { get; set; }
    public List<DataItemColumnData> ColumnDataArray { get; set; } = new();
}

/// <summary>
/// DataItemColumnData-データItem列データ
/// 処理概略：DataItemColumnDataに関する処理をまとめる
/// </summary>
public sealed class DataItemColumnData
{
    public string ColumnId { get; set; } = string.Empty;

    // Category/kind identifier (used by RequestId=7 CateNameList extraction)
    public string DataItemKind { get; set; } = string.Empty;

    public bool IsHeader { get; set; }

    // NOTE: original spec has typo "IsNnumeric"
    public bool IsNnumeric { get; set; }
}

/// <summary>
/// GridDataResult-Gridデータ結果
/// 処理概略：GridDataResultに関する処理をまとめる
/// </summary>
public sealed class GridDataResult
{
    /// <summary>
    /// Column-major 2D data: GridData[colIndex][rowIndex]
    /// </summary>
    public IList<ICollection<object?>> GridData { get; set; } = new List<ICollection<object?>>();
}

/// <summary>
/// GridFilterInfoResult-GridFilterInfo結果
/// 処理概略：GridFilterInfoResultに関する処理をまとめる
/// </summary>
public sealed class GridFilterInfoResult
{
    public List<FilterInfoString> FilterInfoList { get; set; } = new();
}

/// <summary>
/// FilterInfoString-FilterInfo文字列
/// 処理概略：FilterInfoStringに関する処理をまとめる
/// </summary>
public sealed class FilterInfoString
{
    public string ColumnName { get; set; } = string.Empty;
    public string FilterString { get; set; } = string.Empty;
    public bool IsCurrentFilter { get; set; }
}