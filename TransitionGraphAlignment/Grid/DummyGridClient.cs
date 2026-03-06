using System.Text;

namespace TransitionGraphAlignment.Grid;

/// <summary>
/// Dummy (CSV-backed) implementation.
///
/// NOTE:
/// - This class intentionally does NOT reference DXC.* assemblies.
///   (Those assemblies may exist both as "dummy" and "production" and can easily be shadowed.)
/// - It returns Alignment's internal models (see GridModels.cs).
/// </summary>
public sealed class DummyGridClient : IGridClient
{
    private readonly string _csvDirectory;

    public DummyGridClient(string csvDirectory)
    {
        _csvDirectory = csvDirectory;
    }

    public Task<ColumnDataAndRowCountResult> GetColumnDataAndRowCountAsync(string secondCacheId, CancellationToken cancellationToken)
    {
        var (header, rows) = LoadCsv(secondCacheId);
        var cols = header.Select(h => new DataItemColumnData { ColumnId = h, DisplayName = h }).ToList();

        return Task.FromResult(new ColumnDataAndRowCountResult
        {
            RowCount = rows.Count,
            ColumnDataArray = cols
        });
    }

    public Task<GridDataResult> GetGridDataAsync(string[] columnIds, string gridId, bool isGetVirtualColumnValue, int height, int width, int x, int y, CancellationToken cancellationToken)
    {
        var (header, rows) = LoadCsv(gridId);

        // Column selection
        var requested = (columnIds == null || columnIds.Length == 0)
            ? header
            : columnIds;

        var headerIndex = header
            .Select((name, idx) => (name, idx))
            .ToDictionary(t => t.name, t => t.idx, StringComparer.Ordinal);

        var startRow = Math.Max(0, y);
        var takeRows = height > 0 ? height : rows.Count;
        var slice = rows.Skip(startRow).Take(takeRows).ToList();

        var gridData = new List<ICollection<object?>>();
        foreach (var r in slice)
        {
            var outRow = new List<object?>();
            foreach (var col in requested)
            {
                if (!headerIndex.TryGetValue(col, out var idx))
                {
                    outRow.Add(null);
                    continue;
                }

                outRow.Add(idx >= 0 && idx < r.Length ? r[idx] : null);
            }
            gridData.Add(outRow);
        }

        // For dummy, ColumnIndexArray is "0..N-1" for returned columns.
        var colIndexArray = Enumerable.Range(0, requested.Length).ToArray();

        return Task.FromResult(new GridDataResult
        {
            GridData = gridData,
            ColumnIndexArray = colIndexArray
        });
    }

    public Task<GridFilterInfoResult> GetGridFilterInfoAsync(string secondCacheId, CancellationToken cancellationToken)
        => Task.FromResult(new GridFilterInfoResult { FilterInfo = string.Empty });

    public Task<string> SetFilterAndSortAsync(FilterAndSortParam param, CancellationToken cancellationToken)
        => Task.FromResult("Accept Request");

    private (string[] header, List<string?[]> rows) LoadCsv(string key)
    {
        var csvPath = ResolveCsvPath(key);
        var lines = File.ReadAllLines(csvPath, Encoding.UTF8);
        if (lines.Length == 0)
            return (Array.Empty<string>(), new List<string?[]>());

        var header = SplitCsvLine(lines[0]).Select(s => s ?? string.Empty).ToArray();
        var rows = new List<string?[]>();
        for (var i = 1; i < lines.Length; i++)
            rows.Add(SplitCsvLine(lines[i]));

        return (header, rows);
    }

    private string ResolveCsvPath(string key)
    {
        // 1) Exact match like sample-<key>.csv
        var exact = Path.Combine(_csvDirectory, $"sample-{key}.csv");
        if (File.Exists(exact)) return exact;

        // 2) Any file containing key under sample*.csv
        if (Directory.Exists(_csvDirectory))
        {
            var hit = Directory.GetFiles(_csvDirectory, "sample*.csv")
                .FirstOrDefault(p => p.Contains(key, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(hit)) return hit;
        }

        // 3) Default
        return Path.Combine(_csvDirectory, "sample.csv");
    }

    // Very small CSV splitter: supports quoted fields and escaped quotes.
    private static string?[] SplitCsvLine(string line)
    {
        var result = new List<string?>();
        var sb = new StringBuilder();
        var inQuote = false;

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"')
            {
                if (inQuote && i + 1 < line.Length && line[i + 1] == '"')
                {
                    sb.Append('"');
                    i++;
                }
                else
                {
                    inQuote = !inQuote;
                }
                continue;
            }

            if (c == ',' && !inQuote)
            {
                result.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            sb.Append(c);
        }

        result.Add(sb.ToString());
        return result.ToArray();
    }
}
