using System.Globalization;
using System.Linq;
using System.Text.Json;
using TransitionGraph.Dtos;
using TransitionGraphAlignment.Grid;
using System.IO;

namespace TransitionGraphAlignment;

/// <summary>
/// RequestId=8 builder (CSV-backed via DummyDLL GridClient).
///
/// Implements:
/// - Lot stats creation from grid rows (Lot/Wafer rows supported)
/// - Wafer list creation when WAFER_ID exists and XAxis/GridGranularity indicates Wafer
/// - HistoryInfo pickup from columns:
///     履歴測定|...|装置ID, 履歴測定|...|JOB, 履歴MES|...|装置ID, 履歴MES|...|レシピNo
/// - AllStats aggregation from LotStats
/// - MainLot Spec/Limit pickup (+Spec/-Spec/+Limit/-Limit) with nullable fields
/// </summary>
public sealed class Request8Builder
{
    private readonly IGridClient _grid;

    public Request8Builder(IGridClient grid)
    {
        _grid = grid;
    }

    public async Task<GraphDataDto> BuildAsync(
        string guid,
        IReadOnlyDictionary<string, string> parameters,
        CancellationToken ct)
    {
        // Parameters mapping (TransitionGraphAPI default mapping => Parm1..ParmN)
        // Parm1: GUID (also used as SecondCacheId for Dummy CSV)
        // Parm2: lot list json string (e.g. ["LOT1","LOT2"])
        // Parm3: itemName
        // Parm4: xAxis (Lot/Wafer)
        // Parm5: mainLotName
        // Parm6: categoryName (reserved)
        // Parm7: grid granularity (Lot/Wafer/Chip)

        var parm1 = Get(parameters, "Parm1");
        if (!string.IsNullOrWhiteSpace(parm1))
            guid = parm1;

        var lotsJson = Get(parameters, "Parm2");
        var itemName = Get(parameters, "Parm3");
        var xAxis = Get(parameters, "Parm4") ?? string.Empty;
        var mainLot = Get(parameters, "Parm5") ?? string.Empty;
        var gridGranularity = Get(parameters, "Parm7") ?? string.Empty;


		// If Parm3 already includes a stat suffix like "...|Value", strip it to a base prefix "...|" for column lookup.
		// This makes RequestId=8/9 robust when callers pass an item name copied from RequestId=7 itemNameList.
		var baseItemName = NormalizeBaseItemName(itemName);

        File.AppendAllText(@"C:\temp\tg_grid_impl.txt" ,$"gridImpl={_grid.GetType().FullName}\r\n");

        if (string.IsNullOrWhiteSpace(itemName))
            throw new ArgumentException("Parm3(itemName) is required for RequestId=8");
        if (string.IsNullOrWhiteSpace(lotsJson))
            throw new ArgumentException("Parm2(lot list json) is required for RequestId=8");

        var selectedLots = ParseLotList(lotsJson);
        if (selectedLots.Count == 0)
            throw new ArgumentException("Selected lot list is empty");

        // Determine CSV file key for RequestId=8. (parameter[9] => Parm10)
        // parameter[9] should contain the CSV file name (relative key), typically without extension.
        // Example: "ID8_test002" (will resolve to ID8_test002.csv under DummyDLL/CSV).
        var csvKey = NullIfBlank(Get(parameters, "Parm10"));
        if (!string.IsNullOrWhiteSpace(csvKey) && csvKey.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            csvKey = Path.GetFileNameWithoutExtension(csvKey);

        // Determine second cache id for DummyDLL CSV.
        // Backward compatible fallback: if Parm10 is missing, allow legacy keys or GUID.
        var secondCacheId =
            csvKey
            ?? NullIfBlank(Get(parameters, "SecondCacheId"))
            ?? NullIfBlank(Get(parameters, "secondCacheId"))
            ?? guid;

        //Console.WriteLine($"[ID8] csvKey={csvKey ?? "(null)"}, SecondCacheId(param)={Get(parameters, "SecondCacheId") ?? Get(parameters, "secondCacheId") ?? "(null)"}, secondCacheId(final)={secondCacheId}");
        //_logger.LogInformation("[ID8] csvKey={CsvKey}, SecondCacheId(param)={SecondCacheIdParam}, secondCacheId(final)={Final}",
        //    csvKey,Get(parameters, "SecondCacheId") ?? Get(parameters, "secondCacheId") ?? "(null)",secondCacheId);

        // Load column metadata + grid data
        try
        {
            var colMeta = await _grid.GetColumnDataAndRowCountAsync(secondCacheId, ct);

            // 列数
            var width = colMeta.ColumnDataArray.Count;
            // 行数
            var height = colMeta.RowCount;
            // 列情報（検索文字列）
            var displayColumn = colMeta.ColumnDataArray.Select(c => c.ColumnId).ToList();

            var p = @"C:\temp\tg_id8_debug.txt";
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            File.AppendAllText(p,
                $"[{DateTime.Now:O}] secondCacheId={secondCacheId}\r\n" +
                $"colCount={colMeta.ColumnDataArray.Count}\r\n" +
                $"first20={string.Join(",", colMeta.ColumnDataArray.Take(20).Select(c => c.ColumnId))}\r\n\r\n");

			// Debug: confirm what we received from APIClient and what baseItemName becomes.
			File.AppendAllText(p,
				$"itemName(raw)={itemName}\r\n" +
				$"baseItemName={baseItemName}\r\n" +
				$"itemName(cp)={DumpCodepoints(itemName)}\r\n" +
				$"baseItemName(cp)={DumpCodepoints(baseItemName)}\r\n\r\n");

			// Debug: log the flag we pass into RealGridClient.GetGridDataAsync.
			const bool getVirt = true;
			File.AppendAllText(p, $"GetGridDataAsync.called(isGetVirtualColumnValue)={getVirt}\r\n\r\n");

			//var grid = await _grid.GetGridDataAsync(Array.Empty<string>(), secondCacheId, getVirt, 0, 0, width, height, ct);
			var grid = await _grid.GetGridDataAsync(displayColumn.ToArray(), secondCacheId, getVirt, height, width, 0, 0,  ct);

            var cols = (grid.GridData as IList<ICollection<object?>>) ?? grid.GridData.ToList();

            int lotNameCol = FindColumnIndex(colMeta.ColumnDataArray, "LOT_NAME");
            int waferIdCol = FindColumnIndex(colMeta.ColumnDataArray, "WAFER_ID");

            // Stat columns
            int minCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "MIN");
            int maxCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "MAX");
            int avgCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "Value");
            int medCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "Median");
            int cntCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "Count");
            int stdCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "STD");
            int iqrCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "IQR");

            // Spec/Limit columns
            int plsSpecCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "+Spec");
            int misSpecCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "-Spec");
            int plsLimCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "+Limit");
            int misLimCol = FindColumnIndex(colMeta.ColumnDataArray, baseItemName + "-Limit");

			// Debug: log resolved column indices and the exact lookup keys.
			File.AppendAllText(p,
				$"lookupKeys:\r\n" +
				$"  MIN={baseItemName}MIN\r\n" +
				$"  MAX={baseItemName}MAX\r\n" +
				$"  Value={baseItemName}Value\r\n" +
				$"  Median={baseItemName}Median\r\n" +
				$"  Count={baseItemName}Count\r\n" +
				$"  STD={baseItemName}STD\r\n" +
				$"  IQR={baseItemName}IQR\r\n" +
				$"  +Spec={baseItemName}+Spec\r\n" +
				$"  -Spec={baseItemName}-Spec\r\n" +
				$"  +Limit={baseItemName}+Limit\r\n" +
				$"  -Limit={baseItemName}-Limit\r\n" +
				$"indices:\r\n" +
				$"  LOT_NAME={lotNameCol}, WAFER_ID={waferIdCol}\r\n" +
				$"  MIN={minCol}, MAX={maxCol}, Value={avgCol}, Median={medCol}, Count={cntCol}, STD={stdCol}, IQR={iqrCol}\r\n" +
				$"  +Spec={plsSpecCol}, -Spec={misSpecCol}, +Limit={plsLimCol}, -Limit={misLimCol}\r\n\r\n");

            // History columns (optional)
            FindHistoryColumns(
                colMeta.ColumnDataArray,
                out var measureEquipCol,
                out var measureJobCol,
                out var mesEquipCol,
                out var mesRecipeCol);

            var result = new GraphDataDto { ItemName = itemName };

            // Parm4 / parameter[3] indicates whether the response should be aligned by Lot or Wafer.
            // Lot: wafer DTO should be present as a placeholder (missing) even if wafer rows are not requested.
            bool isLotAxis = string.Equals(xAxis, "Lot", StringComparison.OrdinalIgnoreCase);
            bool isWaferAxis = string.Equals(xAxis, "Wafer", StringComparison.OrdinalIgnoreCase);

            if (lotNameCol < 0 || lotNameCol >= cols.Count)
                return result;

            // Row indices for selected lots (exclude null/empty lot name)
            var lotNameColumn = cols[lotNameCol].ToList();
            var selectedRowIndices = lotNameColumn
                .Select((v, i) => new { v, i })
                .Where(x => !string.IsNullOrWhiteSpace(x.v?.ToString())
                            && selectedLots.Contains(x.v!.ToString()!, StringComparer.OrdinalIgnoreCase))
                .Select(x => x.i)
                .ToList();

            if (selectedRowIndices.Count == 0)
            {
                // Missing handling: return placeholder LotDto (+ WaferDto when aligned by Lot/Wafer)
                foreach (var lot in selectedLots.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    var lotDto = new LotDto { LotName = lot };
                    if (isLotAxis || isWaferAxis)
                        lotDto.Wafers.Add(new WaferDto());
                    // Stats remains default (DataCount=0) => treated as missing.
                    result.Lots.Add(lotDto);
                }

                // Still build Spec/Limit if possible (even when no valid stats exist)
                result.AllStats = BuildAllStats(result.Lots, mainLot, cols, lotNameCol, plsSpecCol, misSpecCol, plsLimCol, misLimCol);
                return result;
            }

            // Group by lot name
            var lotGroups = selectedRowIndices
                .GroupBy(ri => ToStringSafe(GetCell(cols, lotNameCol, ri)), StringComparer.OrdinalIgnoreCase)
                .Where(g => !string.IsNullOrWhiteSpace(g.Key))
                .ToList();

            bool wantWafer =
                waferIdCol >= 0 &&
                (string.Equals(xAxis, "Wafer", StringComparison.OrdinalIgnoreCase)
                 || string.Equals(gridGranularity, "Wafer", StringComparison.OrdinalIgnoreCase));

            foreach (var g in lotGroups)
            {
                var lotDto = new LotDto { LotName = g.Key };

                // Build wafer list (optional)
                if (wantWafer)
                {
                    foreach (var ri in g)
                    {
                        var waferId = ToStringSafe(GetCell(cols, waferIdCol, ri));
                        if (string.IsNullOrWhiteSpace(waferId))
                            continue;

                        var waferStats = BuildStats(cols, ri, minCol, maxCol, avgCol, medCol, cntCol, stdCol, iqrCol);
                        // HistoryInfo
                        AddIfNotEmpty(waferStats.HistoryInfo, GetCellString(cols, measureEquipCol, ri));
                        AddIfNotEmpty(waferStats.HistoryInfo, GetCellString(cols, measureJobCol, ri));
                        AddIfNotEmpty(waferStats.HistoryInfo, GetCellString(cols, mesEquipCol, ri));
                        AddIfNotEmpty(waferStats.HistoryInfo, GetCellString(cols, mesRecipeCol, ri));

                        lotDto.Wafers.Add(new WaferDto { WaferId = waferId, Stats = waferStats });
                    }
                }

                // Lot stats:
                // - Wafer axis: build from wafer data; if wafer data is missing, keep as missing and add placeholder wafer.
                // - Lot axis (or when wafer rows are not requested): use the first row.
                if (lotDto.Wafers.Count > 0)
                {
                    lotDto.Stats = AggregateStats(lotDto.Wafers.Select(w => w.Stats).ToList());

                    // LotのHistoryInfoは最初のWaferのHistoryInfoと同じにする（欠損は AggregateStats 側で除外）
                    var firstWithData = lotDto.Wafers.FirstOrDefault(w => w.Stats.DataCount > 0);
                    if (firstWithData is not null)
                        lotDto.Stats.HistoryInfo = firstWithData.Stats.HistoryInfo.ToList();
                }
                else if (isWaferAxis && wantWafer)
                {
                    // Wafer axis but no wafer data collected => missing
                    lotDto.Wafers.Add(new WaferDto());
                    lotDto.Stats = new StatsDto();
                }
                else
                {
                    var ri = g.First();
                    lotDto.Stats = BuildStats(cols, ri, minCol, maxCol, avgCol, medCol, cntCol, stdCol, iqrCol);
                }

                // Lot axis: always include a placeholder wafer DTO (shape 유지)
                if (isLotAxis && lotDto.Wafers.Count == 0)
                    lotDto.Wafers.Add(new WaferDto());

                result.Lots.Add(lotDto);
            }

            // Add missing lots that had no rows in the grid (shape 유지)
            var existingLotSet = result.Lots
                .Select(l => l.LotName)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var lot in selectedLots.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if (existingLotSet.Contains(lot)) continue;
                var lotDto = new LotDto { LotName = lot };
                if (isLotAxis || isWaferAxis)
                    lotDto.Wafers.Add(new WaferDto());
                result.Lots.Add(lotDto);
            }

            // AllStats aggregation from lot list
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

    private static StatsDto BuildStats(
        IList<ICollection<object?>> cols,
        int ri,
        int minCol,
        int maxCol,
        int avgCol,
        int medCol,
        int cntCol,
        int stdCol,
        int iqrCol)
    {
        return new StatsDto
        {
            Minimum = ToDouble(GetCell(cols, minCol, ri)),
            Maximum = ToDouble(GetCell(cols, maxCol, ri)),
            Average = ToDouble(GetCell(cols, avgCol, ri)),
            Median = ToDouble(GetCell(cols, medCol, ri)),
            DataCount = (int)ToDouble(GetCell(cols, cntCol, ri)),
            Sigma = ToDouble(GetCell(cols, stdCol, ri)),
            IQR = ToDouble(GetCell(cols, iqrCol, ri)),
        };
    }

    private static StatsDto AggregateStats(List<StatsDto> statsList)
    {
        // Exclude missing (DataCount==0) from aggregation.
        var valid = statsList.Where(s => s.DataCount > 0).ToList();
        if (valid.Count == 0) return new StatsDto();

        var mins = valid.Select(s => s.Minimum).ToList();
        var maxs = valid.Select(s => s.Maximum).ToList();
        var avgs = valid.Select(s => s.Average).ToList();
        var meds = valid.Select(s => s.Median).ToList();
        var sigs = valid.Select(s => s.Sigma).ToList();
        var iqrs = valid.Select(s => s.IQR).ToList();
        var cnts = valid.Select(s => s.DataCount).ToList();

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
        // Exclude missing lots (DataCount==0) from aggregation.
        var validLots = lots.Where(l => l.Stats.DataCount > 0).ToList();
        if (validLots.Count > 0)
        {
            var mins = validLots.Select(l => l.Stats.Minimum).ToList();
            var maxs = validLots.Select(l => l.Stats.Maximum).ToList();
            var avgs = validLots.Select(l => l.Stats.Average).ToList();
            var meds = validLots.Select(l => l.Stats.Median).ToList();
            var sigs = validLots.Select(l => l.Stats.Sigma).ToList();
            var iqrs = validLots.Select(l => l.Stats.IQR).ToList();
            var counts = validLots.Select(l => l.Stats.DataCount).ToList();

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
        }

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
                double? plsSpec = (plsSpecCol >= 0) ? ToDouble(GetCell(cols, plsSpecCol, targetRow)) : null;
                double? misSpec = (misSpecCol >= 0) ? ToDouble(GetCell(cols, misSpecCol, targetRow)) : null;
                double? plsLim = (plsLimCol >= 0) ? ToDouble(GetCell(cols, plsLimCol, targetRow)) : null;
                double? misLim = (misLimCol >= 0) ? ToDouble(GetCell(cols, misLimCol, targetRow)) : null;

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

private static void FindHistoryColumns(
    IReadOnlyList<DataItemColumnData> cols,
    out int measureEquip,
    out int measureJob,
    out int mesEquip,
    out int mesRecipe)
{
    measureEquip = -1;
    measureJob = -1;
    mesEquip = -1;
    mesRecipe = -1;

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
        if (string.IsNullOrWhiteSpace(id)) continue;

        var parts = id.Split('|', StringSplitOptions.RemoveEmptyEntries)
                      .Select(Normalize)
                      .ToArray();
        if (parts.Length == 0) continue;

        var head = parts[0];
        var tail = parts[^1];

        // Only head/tail matter; tolerate any middle parts.
        bool isMeasure = string.Equals(head, "履歴測定", StringComparison.Ordinal) || id.Contains("履歴測定", StringComparison.Ordinal);
        bool isMes = string.Equals(head, "履歴MES", StringComparison.Ordinal) || id.Contains("履歴MES", StringComparison.Ordinal);

        if (isMeasure && string.Equals(tail, "装置ID", StringComparison.Ordinal) && measureEquip < 0) measureEquip = i;
        if (isMeasure && string.Equals(tail, "JOB", StringComparison.Ordinal) && measureJob < 0) measureJob = i;
        if (isMes && string.Equals(tail, "装置ID", StringComparison.Ordinal) && mesEquip < 0) mesEquip = i;
        if (isMes && string.Equals(tail, "レシピNo", StringComparison.Ordinal) && mesRecipe < 0) mesRecipe = i;
    }
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

    private static string NormalizeBaseItemName(string? itemName)
    {
        var s = (itemName ?? string.Empty)
            .Replace('｜', '|')
            .Trim()
            .Trim('　');

        if (s.Length == 0)
            return s;

        // If already a base prefix like "...|", keep it as-is.
        if (s.EndsWith("|", StringComparison.Ordinal))
            return s;

        // If the last token looks like a stats suffix (Value/MAX/MIN/...), strip it to "...|".
        var suffixes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Value","MAX","MIN","Median","STD","Count","IQR",
            "+Spec","-Spec","+Limit","-Limit","+σ","-σ","Cp","Cpk"
        };

        var lastBar = s.LastIndexOf('|');
        if (lastBar >= 0)
        {
            var lastToken = s[(lastBar + 1)..];
            if (suffixes.Contains(lastToken))
                return s[..(lastBar + 1)];
        }

        return s;
    }

    private static string DumpCodepoints(string? s)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        var sb = new System.Text.StringBuilder();
        foreach (var ch in s)
        {
            if (sb.Length > 0) sb.Append(' ');
            sb.Append("U+");
            sb.Append(((int)ch).ToString("X4"));
        }
        return sb.ToString();
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

    private static string? NullIfBlank(string? s)
        => string.IsNullOrWhiteSpace(s) ? null : s;

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

    private static string GetCellString(IList<ICollection<object?>> cols, int colIndex, int rowIndex)
        => ToStringSafe(GetCell(cols, colIndex, rowIndex));

    private static void AddIfNotEmpty(List<string> list, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
            list.Add(value);
    }

    private static double ToDouble(object? v)
    {
        if (v is null) return 0d;
        if (v is double d) return d;
        if (v is float f) return f;
        if (v is int i) return i;
        if (double.TryParse(v.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var r)) return r;
        if (double.TryParse(v.ToString(), NumberStyles.Float, CultureInfo.CurrentCulture, out r)) return r;
        return 0d;
    }

    private static double Median(List<double> values)
    {
        if (values.Count == 0) return 0d;
        values.Sort();
        int mid = values.Count / 2;
        if (values.Count % 2 == 1) return values[mid];
        return (values[mid - 1] + values[mid]) / 2.0;
    }
}
