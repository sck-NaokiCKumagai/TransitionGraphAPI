using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using TransitionGraph.Dtos;
using TransitionGraphAlignment.Grid;
using System.IO;

namespace TransitionGraphAlignment;

/// <summary>
/// RequestId=9 builder.
///
/// ID9 is almost the same as ID8, but builds Chip data and aggregates:
/// Chip (value) -> WaferInChipDto stats -> LotDto stats -> AllStatsDto.
///
/// Notes:
/// - Chip value column is identified by a '|' separated ColumnId. When split length &gt; 1,
///   (head + tail) is compared with (baseItemName + "Value").
/// - When no chip data exists for a wafer/lot, Stats is returned as new StatsDto() (DataCount=0).
/// - Spec/Limit pickup is attempted like ID8; if columns/rows are not available, values remain default.
/// </summary>
public sealed class Request9Builder
{
    private readonly IGridClient _grid;

    public Request9Builder(IGridClient grid)
    {
        _grid = grid;
    }

    public async Task<GraphDataDto> BuildAsync(
        string guid,
        IReadOnlyDictionary<string, string> parameters,
        CancellationToken ct)
    {
        // Parm1: GUID (also used as SecondCacheId for Dummy CSV)
        // Parm2: lot list json string (e.g. ["LOT1","LOT2"])
        // Parm3: itemName
        // Parm4: xAxis (Lot/Wafer/Chip) - ID9 expects Chip rows
        // Parm5: mainLotName
        // Parm6: categoryName (reserved)
        // Parm7: grid granularity (Lot/Wafer/Chip)

        var parm1 = Get(parameters, "Parm1");
        if (!string.IsNullOrWhiteSpace(parm1))
            guid = parm1;

        var lotsJson = Get(parameters, "Parm2");
        var itemName = Get(parameters, "Parm3");
        var baseItemName = NormalizeBaseItemName(itemName);
        var mainLot = Get(parameters, "Parm5") ?? string.Empty;

        if (string.IsNullOrWhiteSpace(itemName))
            throw new ArgumentException("Parm3(itemName) is required for RequestId=9");
        if (string.IsNullOrWhiteSpace(lotsJson))
            throw new ArgumentException("Parm2(lot list json) is required for RequestId=9");

        var selectedLots = ParseLotList(lotsJson);
        if (selectedLots.Count == 0)
            throw new ArgumentException("Selected lot list is empty");

        // Determine CSV file key for RequestId=9. (parameter[9] => Parm10)
        // parameter[9] should contain the CSV file name (relative key), typically without extension.
        // Example: "ID8_test002" (will resolve to ID8_test002.csv under DummyDLL/CSV).
        var csvKey = Get(parameters, "Parm10");
        if (!string.IsNullOrWhiteSpace(csvKey) && csvKey.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            csvKey = Path.GetFileNameWithoutExtension(csvKey);

        // Determine second cache id for DummyDLL CSV.
        // Backward compatible fallback: if Parm10 is missing, allow legacy keys or GUID.
        var secondCacheId =
            csvKey
            ?? Get(parameters, "SecondCacheId")
            ?? Get(parameters, "secondCacheId")
            ?? guid;

        // Load column metadata + grid data
        try
        {
            var p = @"C:\temp\tg_id8_debug.txt";
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            var colMeta = await _grid.GetColumnDataAndRowCountAsync(secondCacheId, ct);

            // 列数
            var width = colMeta.ColumnDataArray.Count;
            // 行数
            var height = colMeta.RowCount;
            // 列情報（検索文字列）
            var displayColumn = colMeta.ColumnDataArray.Select(c => c.ColumnId).ToList();

            const bool getVirt = true;

            File.AppendAllText(p,
            $"colCount={colMeta.ColumnDataArray.Count}\r\n" +
            $"first20={string.Join(",", colMeta.ColumnDataArray.Take(20).Select(c => c.ColumnId))}\r\n\r\n");

            //var grid = await _grid.GetGridDataAsync(displayColumn.ToArray(), secondCacheId, false, 0, 0, width, height, ct);
            var grid = await _grid.GetGridDataAsync(displayColumn.ToArray(), secondCacheId, getVirt, height, width, 0, 0, ct);

            var cols = (grid.GridData as IList<ICollection<object?>>) ?? grid.GridData.ToList();

            int lotNameCol = FindColumnIndex(colMeta.ColumnDataArray, "LOT_NAME");
            int waferIdCol = FindColumnIndex(colMeta.ColumnDataArray, "WAFER_ID");
            int chipIdCol = FindColumnIndex(colMeta.ColumnDataArray, "CHIP_ID");

            // Value column (chip raw data for ID9)
            int valueCol = FindValueColumnIndex(colMeta.ColumnDataArray, baseItemName);

            // Spec/Limit columns (attempt like ID8)
            int plsSpecCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "+Spec");
            int misSpecCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "-Spec");
            int plsLimCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "+Limit");
            int misLimCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "-Limit");

            var result = new GraphDataDto { ItemName = itemName };

            if (lotNameCol < 0 || lotNameCol >= cols.Count)
                return result;

            // Row indices for selected lots
            var lotNameColumn = cols[lotNameCol].ToList();
            var selectedRowIndices = lotNameColumn
                .Select((v, i) => new { v, i })
                .Where(x => !string.IsNullOrWhiteSpace(x.v?.ToString())
                            && selectedLots.Contains(x.v!.ToString()!, StringComparer.OrdinalIgnoreCase))
                .Select(x => x.i)
                .ToList();

            if (selectedRowIndices.Count == 0)
            {
                result.AllStats = BuildAllStats(result.Lots, mainLot, cols, lotNameCol, plsSpecCol, misSpecCol, plsLimCol, misLimCol);
                return result;
            }

            var lotGroups = selectedRowIndices
                .GroupBy(ri => ToStringSafe(GetCell(cols, lotNameCol, ri)), StringComparer.OrdinalIgnoreCase)
                .Where(g => !string.IsNullOrWhiteSpace(g.Key))
                .ToList();

            foreach (var lotGroup in lotGroups)
            {
                var lotDto = new LotDto { LotName = lotGroup.Key };

                // Wafer grouping inside a lot
                var waferGroups = lotGroup
                    .GroupBy(ri => waferIdCol >= 0 ? ToStringSafe(GetCell(cols, waferIdCol, ri)) : string.Empty, StringComparer.OrdinalIgnoreCase)
                    .Where(g => !string.IsNullOrWhiteSpace(g.Key))
                    .ToList();

                foreach (var waferGroup in waferGroups)
                {
                    var wafer = new WaferInChipDto { WaferId = waferGroup.Key };

                    // Build chips
                    foreach (var ri in waferGroup)
                    {
                        var chipId = chipIdCol >= 0 ? ToStringSafe(GetCell(cols, chipIdCol, ri)) : string.Empty;
                        if (string.IsNullOrWhiteSpace(chipId))
                            continue;

                        // IMPORTANT:
                        // Even when the value is missing (null/blank/unparsable), we must still return
                        // a ChipDto entry (new ChipDto()) so the client can keep the chip list shape.
                        // Missing value is represented as Data = null.
                        var data = TryToDoubleNullable(GetCell(cols, valueCol, ri));
                        wafer.ChipDto.Add(new Chip9Dto
                        {
                            ChipId = chipId,
                            Data = data
                        });
                    }

                    // Wafer stats from chip data
                    wafer.Stats = BuildStatsFromValues(wafer.ChipDto.Where(c => c.Data.HasValue).Select(c => c.Data!.Value).ToList());

                    lotDto.WaferInChipDto.Add(wafer);
                }

                // Lot stats: aggregate from wafers (ID8 style)
                if (lotDto.WaferInChipDto.Count > 0)
                    lotDto.Stats = AggregateStats(lotDto.WaferInChipDto.Select(w => w.Stats).ToList());
                else
                    lotDto.Stats = new StatsDto();

                result.Lots.Add(lotDto);
            }

            // AllStats aggregation from lot list (+Spec/-Spec/+Limit/-Limit from main lot when possible)
            result.AllStats = BuildAllStats(result.Lots, mainLot, cols, lotNameCol, plsSpecCol, misSpecCol, plsLimCol, misLimCol);
            return result;
        }
        catch (Exception ex)
        {
            var p = @"C:\temp\tg_id8_debug.txt";
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            File.AppendAllText(p,
                $"[EX] {ex.GetType().FullName}\r\n{ex.Message}\r\n{ex}\r\n\r\n");
            throw;
        }
    }

    private static StatsDto BuildStatsFromValues(List<double> values)
    {
        if (values.Count == 0)
            return new StatsDto();

        values.Sort();
        var min = values[0];
        var max = values[^1];
        var avg = values.Average();
        var med = MedianSorted(values);
        var sigma = StdDev(values, avg);
        var iqr = IqrSorted(values);

        return new StatsDto
        {
            Minimum = min,
            Maximum = max,
            Average = avg,
            Median = med,
            DataCount = values.Count,
            Sigma = sigma,
            IQR = iqr
        };
    }

    private static StatsDto AggregateStats(List<StatsDto> statsList)
    {
        if (statsList.Count == 0) return new StatsDto();

        var mins = statsList.Select(s => s.Minimum).ToList();
        var maxs = statsList.Select(s => s.Maximum).ToList();
        var avgs = statsList.Select(s => s.Average).ToList();
        var meds = statsList.Select(s => s.Median).ToList();
        var sigs = statsList.Select(s => s.Sigma).ToList();
        var iqrs = statsList.Select(s => s.IQR).ToList();
        var cnts = statsList.Select(s => s.DataCount).ToList();

        return new StatsDto
        {
            Minimum = mins.Min(),
            Maximum = maxs.Max(),
            Average = avgs.Average(),
            Median = Median(meds),
            DataCount = cnts.Sum(),
            Sigma = sigs.Average(),
            IQR = iqrs.Average(),
        };
    }

    private static AllStatsDto BuildAllStats(
        List<LotDto> lots,
        string mainLot,
        IList<ICollection<object?>> cols,
        int lotNameCol,
        int plsSpecCol,
        int misSpecCol,
        int plsLimCol,
        int misLimCol)
    {
        var all = new AllStatsDto();
        if (lots.Count == 0) return all;

        var mins = lots.Select(l => l.Stats.Minimum).ToList();
        var maxs = lots.Select(l => l.Stats.Maximum).ToList();
        var avgs = lots.Select(l => l.Stats.Average).ToList();
        var meds = lots.Select(l => l.Stats.Median).ToList();
        var sigs = lots.Select(l => l.Stats.Sigma).ToList();
        var iqrs = lots.Select(l => l.Stats.IQR).ToList();
        var counts = lots.Select(l => l.Stats.DataCount).ToList();

        all.Minimum = mins.Min();
        all.Maximum = maxs.Max();
        all.Average = avgs.Average();
        all.Median = Median(meds);
        all.DataCount = counts.Sum();
        all.Sigma = sigs.Average();
        all.IQR = iqrs.Average();
        all.Min_Average = avgs.Min();
        all.Max_Average = avgs.Max();
        all.Min_Median = meds.Min();
        all.Max_Median = meds.Max();

        // Spec/Limit from main lot row (if found)
        if (!string.IsNullOrWhiteSpace(mainLot) && lotNameCol >= 0 && lotNameCol < cols.Count)
        {
            var lotNameColumn = cols[lotNameCol].ToList();
            var targetRow = lotNameColumn
                .Select((v, i) => new { v, i })
                .Where(x => string.Equals(x.v?.ToString(), mainLot, StringComparison.OrdinalIgnoreCase))
                .Select(x => x.i)
                .DefaultIfEmpty(-1)
                .First();

            if (targetRow >= 0)
            {
                double? plsSpec = (plsSpecCol >= 0) ? TryToDoubleNullable(GetCell(cols, plsSpecCol, targetRow)) : null;
                double? misSpec = (misSpecCol >= 0) ? TryToDoubleNullable(GetCell(cols, misSpecCol, targetRow)) : null;
                double? plsLim = (plsLimCol >= 0) ? TryToDoubleNullable(GetCell(cols, plsLimCol, targetRow)) : null;
                double? misLim = (misLimCol >= 0) ? TryToDoubleNullable(GetCell(cols, misLimCol, targetRow)) : null;

                all.PlsSPECMainLot = plsSpec;
                all.MisSPECMainLot = misSpec;
                all.PlsLIMITMainLot = plsLim;
                all.MisLIMITMainLot = misLim;

                // legacy fields
                if (plsSpec is not null) all.PlsSPEC = plsSpec.Value;
                if (misSpec is not null) all.MisSPEC = misSpec.Value;
                if (plsLim is not null) all.PlsLimit = plsLim.Value;
                if (misLim is not null) all.MisLimit = misLim.Value;
            }
        }

        return all;
    }

    private static int FindColumnIndex(IReadOnlyList<DataItemColumnData> cols, string columnId)
    {
        for (int i = 0; i < cols.Count; i++)
        {
            if (string.Equals(cols[i].ColumnId, columnId, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    /// <summary>
    /// Value column finder for ID9.
    /// ColumnId can be plain (e.g. LOT_NAME) or pipe-separated (e.g. "項目名|...|Value").
    /// If pipe-separated and (head+tail) equals (baseItemName + "Value"), it matches.
    /// </summary>
    private static int FindValueColumnIndex(IReadOnlyList<DataItemColumnData> cols, string itemName)
    {
        var target = (itemName ?? string.Empty) + "Value";

        static string Normalize(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            // Full-width pipe to half-width
            s = s.Replace('｜', '|');
            // Trim ASCII and full-width spaces
            s = s.Trim().Trim('　');
            return s;
        }

        for (int i = 0; i < cols.Count; i++)
        {
            var raw = cols[i].ColumnId ?? string.Empty;
            var id = Normalize(raw);
            if (string.IsNullOrWhiteSpace(id))
                continue;

            // Non pipe => ignore (spec says array length 1 => non-target)
            var parts = id.Split('|', StringSplitOptions.None)
                          .Select(Normalize)
                          .Where(p => p.Length > 0)
                          .ToArray();
            if (parts.Length <= 1)
                continue;

            var combined = parts[0] + parts[^1];
            if (string.Equals(combined, target, StringComparison.Ordinal))
                return i;
        }
        return -1;
    }

    private static object? GetCell(IList<ICollection<object?>> cols, int colIndex, int rowIndex)
    {
        if (colIndex < 0 || colIndex >= cols.Count) return null;
        var col = cols[colIndex];
        if (col is IList<object?> list)
        {
            if (rowIndex < 0 || rowIndex >= list.Count) return null;
            return list[rowIndex];
        }

        int i = 0;
        foreach (var v in col)
        {
            if (i == rowIndex) return v;
            i++;
        }
        return null;
    }

    private static string? Get(IReadOnlyDictionary<string, string> dict, string key)
        => dict.TryGetValue(key, out var v) ? v : null;

    private static List<string> ParseLotList(string jsonOrValue)
    {
        try
        {
            var arr = JsonSerializer.Deserialize<List<string>>(jsonOrValue);
            if (arr is not null) return arr.Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
        }
        catch
        {
            // ignore
        }

        return jsonOrValue
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();
    }

    private static string ToStringSafe(object? v) => v?.ToString() ?? string.Empty;

    private static double? TryToDoubleNullable(object? v)
    {
        if (v is null) return null;
        if (v is double d) return d;
        if (v is float f) return f;
        if (v is int i) return i;

        var s = v.ToString();
        if (string.IsNullOrWhiteSpace(s)) return null;

        if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var r)) return r;
        if (double.TryParse(s, NumberStyles.Float, CultureInfo.CurrentCulture, out r)) return r;
        return null;
    }

    private static double Median(List<double> values)
    {
        if (values.Count == 0) return 0d;
        values.Sort();
        return MedianSorted(values);
    }

    private static double MedianSorted(IReadOnlyList<double> sorted)
    {
        if (sorted.Count == 0) return 0d;
        int mid = sorted.Count / 2;
        if (sorted.Count % 2 == 1) return sorted[mid];
        return (sorted[mid - 1] + sorted[mid]) / 2.0;
    }

    // Population std deviation (divide by N)
    private static double StdDev(IReadOnlyList<double> values, double mean)
    {
        if (values.Count == 0) return 0d;
        double sum = 0d;
        for (int i = 0; i < values.Count; i++)
        {
            var diff = values[i] - mean;
            sum += diff * diff;
        }
        return Math.Sqrt(sum / values.Count);
    }

    // Tukey-style IQR using median-of-halves on sorted list
    private static double IqrSorted(IReadOnlyList<double> sorted)
    {
        if (sorted.Count < 4) return 0d;

        int n = sorted.Count;
        int mid = n / 2;
        IReadOnlyList<double> lower = sorted.Take(mid).ToList();
        IReadOnlyList<double> upper = (n % 2 == 0) ? sorted.Skip(mid).ToList() : sorted.Skip(mid + 1).ToList();

        var q1 = MedianSorted(lower);
        var q3 = MedianSorted(upper);
        return q3 - q1;
    }

    private static string NormalizeBaseItemName(string? itemName)
    {
        if (string.IsNullOrWhiteSpace(itemName)) return string.Empty;

        // Normalize full-width delimiter
        var s = itemName.Replace('｜', '|').Trim();

        // If already looks like a "base" (ends with '|'), keep as-is
        if (s.EndsWith("|", StringComparison.Ordinal)) return s;

        // If it ends with known suffix (Value/MAX/MIN/Median/STD/Count/IQR/+Spec/-Spec/+σ/-σ/Cp/Cpk etc.),
        // cut back to the last '|'
        var lastBar = s.LastIndexOf('|');
        if (lastBar >= 0)
        {
            var tail = s.Substring(lastBar + 1);

            // Add suffixes as needed (based on ID7 itemNameList patterns)
            var known = tail is "Value" or "MAX" or "MIN" or "Median" or "STD" or "Count" or "IQR"
                            or "+Spec" or "-Spec" or "+σ" or "-σ" or "Cp" or "Cpk"
                            or "+Limit" or "-Limit";

            if (known)
            {
                return s.Substring(0, lastBar + 1); // keep trailing '|'
            }
        }

        // Otherwise, return as-is (don't over-normalize)
        return s;
    }
}
