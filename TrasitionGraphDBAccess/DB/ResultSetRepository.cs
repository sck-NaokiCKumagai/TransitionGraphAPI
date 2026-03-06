using System.Data.Common;
using System.Text.Json;

namespace TransitionGraph.Db;

public sealed record PageItem(int RowNo, string Value);
public sealed record PageResult(List<PageItem> Items, int NextLastRowNo, bool Done);

public interface IResultSetRepository
{
    Task<PageResult> FetchPageAsync(Guid resultId, int lastRowNo, int pageSize, CancellationToken ct);

    /// <summary>
    /// ページングを行わず、SQL を 1 回だけ実行して全件取得する。
    /// RowNo は SQL 内で採番されている前提（ROW_NUMBER() OVER ...）。
    /// </summary>
    Task<IReadOnlyList<PageItem>> FetchAllAsync(Guid resultId, CancellationToken ct);
}

public sealed class ResultSetRepository : IResultSetRepository
{
    private readonly IDbConnectionFactory _factory;
    private readonly ISqlDialect _dialect;
    private readonly IQueryRegistry _registry;
    private readonly ISqlCatalog _catalog;
    private readonly ISeekCursorStore _seekCursorStore;
    private readonly string _dataDir;

    public ResultSetRepository(
        IDbConnectionFactory factory,
        ISqlDialect dialect,
        IQueryRegistry registry,
        ISqlCatalog catalog,
        ISeekCursorStore seekCursorStore,
        string dataDir)
    {
        _factory = factory;
        _dialect = dialect;
        _registry = registry;
        _catalog = catalog;
        _seekCursorStore = seekCursorStore;
        _dataDir = dataDir;
    }

    /// <summary>
    /// SQL本文に「完全一致のパラメータトークン」が含まれるか判定する。
    /// token例: ":pageSize" / "@pageSize"
    /// 注意: ":id" が ":resultId" に誤マッチしないよう、後続文字が英数/_ の場合は除外する。
    /// </summary>
    private static bool SqlUsesParam(string sql, string token)
    {
        var idx = 0;
        while (true)
        {
            idx = sql.IndexOf(token, idx, StringComparison.OrdinalIgnoreCase);
            if (idx < 0) return false;

            var next = idx + token.Length;
            if (next >= sql.Length) return true;

            var c = sql[next];
            if (!(char.IsLetterOrDigit(c) || c == '_')) return true;

            idx = next;
        }
    }

    public async Task<PageResult> FetchPageAsync(Guid resultId, int lastRowNo, int pageSize, CancellationToken ct)
    {
        // resultId は OpenQuery 登録された (sqlKey, parameters) を参照するために使用する。
        // まだ登録がない場合は固定 SQL（MEASURE_LOT_HISTORY_1PC）として扱う。
        var items = new List<PageItem>(capacity: Math.Max(0, pageSize));

        // Resolve sqlKey + parameters
        var sqlKey = "MEASURE_LOT_HISTORY_1PC";
        IReadOnlyDictionary<string, string> parameters = new Dictionary<string, string>();
        if (_registry.TryGet(resultId, out var ctx))
        {
            if (!string.IsNullOrWhiteSpace(ctx.SqlKey)) sqlKey = ctx.SqlKey;
            parameters = ctx.Parameters;
        }

        // Build WHERE from allowed parameters
        var allowed = new HashSet<string>(_catalog.GetAllowedParameters(sqlKey), StringComparer.OrdinalIgnoreCase);
        var conds = new List<string>();
        foreach (var kv in parameters)
        {
            if (!allowed.Contains(kv.Key)) continue;
            var col = MapParamToColumn(kv.Key);
            if (col is null) continue;
            conds.Add($"AND {col} = {_dialect.Param(kv.Key)}");
        }

        var whereClause = conds.Count == 0 ? "" : string.Join("\n        ", conds);
        var sqlTemplate = _catalog.GetFetchPageSql(sqlKey);
        var sqlText = sqlTemplate.Replace("{{where}}", whereClause);

        
        // =========================================================
        // Seek paging optimization for Vertica CHIP_MEASURE_DATA_?PC
        // - Use lastRowNo as opaque cursorId, and keep the last keyset in memory.
        // - SQL for this sqlKey returns columns (LOT_NAME, TEST_NAME, WAFER_NO, CHIP_NO, RESULT).
        // =========================================================
        var seekableChipMeasure =
            _dialect is VerticaDialect
            && (sqlKey.Equals("CHIP_MEASURE_DATA_1PC", StringComparison.OrdinalIgnoreCase)
                || sqlKey.Equals("CHIP_MEASURE_DATA_2PC", StringComparison.OrdinalIgnoreCase));

        SeekCursor? seekCursor = null;
        if (seekableChipMeasure && _seekCursorStore.TryGet(resultId, lastRowNo, out var c))
        {
            seekCursor = c;
        }


        await using var conn = await _factory.OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sqlText;

        // SQL本文に含まれるパラメータだけ bind する（Vertica の「余計なパラメータ禁止」対策）
        void AddIfUsed(string name, object? value)
        {
            var token = _dialect.Param(name); // Vertica: ":name" / SQLServer: "@name"
            if (SqlUsesParam(cmd.CommandText, token))
            {
                _dialect.AddParameter(cmd, name, value);
            }
        }

        AddIfUsed("pageSize", pageSize);
        AddIfUsed("lastRowNo", lastRowNo);
        if (seekableChipMeasure)
        {
            // When cursor is missing, start from the beginning.
            var lot = seekCursor?.LastLotName ?? "";
            var test = seekCursor?.LastTestName ?? "";
            var wafer = seekCursor?.LastWaferNo ?? -1;
            var chip = seekCursor?.LastChipNo ?? -1;

            AddIfUsed("lastLotName", lot);
            AddIfUsed("lastTestName", test);
            AddIfUsed("lastWaferNo", wafer);
            AddIfUsed("lastChipNo", chip);
        }

        AddIfUsed("resultId", resultId);

        // Bind query parameters
        foreach (var kv in parameters)
        {
            if (!allowed.Contains(kv.Key)) continue;

            object? val = kv.Value;

            // Minimal type normalization
            if (kv.Key.Equals("partitionMKey", StringComparison.OrdinalIgnoreCase)
                && long.TryParse(kv.Value, out var l))
            {
                val = l;
            }

            // DateTime parameters (screen sends "YYYY/MM/DD HH:MI:SS")
            if ((kv.Key.Equals("startTime", StringComparison.OrdinalIgnoreCase)
                 || kv.Key.Equals("endTime", StringComparison.OrdinalIgnoreCase))
                && DateTime.TryParseExact(
                    kv.Value,
                    "yyyy/MM/dd HH:mm:ss",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out var dt))
            {
                val = dt;
            }

            AddIfUsed(kv.Key, val);
        }

        // === Debug: output SQL + parameters ===
        try
        {
            Directory.CreateDirectory(_dataDir);
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"{DateTime.Now:O} EXEC FetchPage resultId={resultId} lastRowNo={lastRowNo} pageSize={pageSize}");
            sb.AppendLine("SQL:");
            sb.AppendLine(cmd.CommandText);
            sb.AppendLine("PARAMETERS:");
            foreach (DbParameter p in cmd.Parameters)
            {
                var v = p.Value is null || p.Value == DBNull.Value ? "null" : p.Value.ToString();
                sb.AppendLine($"  {p.ParameterName} = {v}");
            }
            sb.AppendLine("----");
            File.AppendAllText(Path.Combine(_dataDir, "dbaccess_sql_exec.log"), sb.ToString());
        }
        catch
        {
            // ignore logging failures
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        if (seekableChipMeasure)
        {
            string lastLot = "";
            string lastTest = "";
            int lastWafer = -1;
            int lastChip = -1;

            var rowIndex = 0;
            while (await reader.ReadAsync(ct))
            {
                // Expected columns: LOT_NAME, TEST_NAME, WAFER_NO, CHIP_NO, RESULT
                var lot = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var test = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var wafer = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
                var chip = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3));
                var result = reader.IsDBNull(4) ? "" : Convert.ToString(reader.GetValue(4)) ?? "";

                var value = $"{lot}\t{test}\t{wafer}\t{chip}\t{result}";
                items.Add(new PageItem(++rowIndex, value));

                lastLot = lot;
                lastTest = test;
                lastWafer = wafer;
                lastChip = chip;
            }

            var doneSeek = items.Count < pageSize;
            var nextCursorId = doneSeek ? 0 : _seekCursorStore.Save(resultId, new SeekCursor(lastLot, lastTest, lastWafer, lastChip));
            if (doneSeek) _seekCursorStore.Clear(resultId);

            return new PageResult(items, nextCursorId, doneSeek);
        }

        while (await reader.ReadAsync(ct))
        {
            // SQLServer: many columns / Vertica: RowNo + Value
            var rowNo = Convert.ToInt32(reader.GetValue(0));

            string valueJson;
            if (reader.FieldCount >= 2 && reader.FieldCount <= 3)
            {
                // Vertica catalog returns (RowNo, Value)
                valueJson = reader.IsDBNull(1) ? "{}" : reader.GetString(1);
            }
            else
            {
                // SQLServer catalog returns the full column list
                var payload = new
                {
                    measureDataKind = reader.IsDBNull(1) ? null : reader.GetString(1),
                    testProcessName = reader.IsDBNull(2) ? null : reader.GetString(2),
                    lotName = reader.IsDBNull(3) ? null : reader.GetString(3),
                    measureStartDate = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                    measureEndDate = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5),
                    partitionMKey = reader.IsDBNull(6) ? (int?)null : Convert.ToInt32(reader.GetValue(6)),
                    program = reader.IsDBNull(7) ? null : reader.GetString(7),
                    equipmentId = reader.IsDBNull(8) ? null : reader.GetString(8),
                    operatorName = reader.IsDBNull(9) ? null : reader.GetString(9),
                    deviceType = reader.IsDBNull(10) ? null : reader.GetString(10),
                    tecKind = reader.IsDBNull(11) ? null : reader.GetString(11),
                    devtypeInfo = reader.IsDBNull(12) ? null : reader.GetString(12),
                    chipCount = reader.IsDBNull(13) ? (int?)null : Convert.ToInt32(reader.GetValue(13)),
                    msExist = reader.IsDBNull(14) ? null : reader.GetString(14),
                    markLotName = reader.IsDBNull(15) ? null : reader.GetString(15),
                    nobinFlag = reader.IsDBNull(16) ? null : reader.GetString(16),
                    stDatetime = reader.IsDBNull(17) ? (DateTime?)null : reader.GetDateTime(17),
                };

                valueJson = JsonSerializer.Serialize(payload);
            }

            items.Add(new PageItem(rowNo, valueJson));
        }


        var done = items.Count < pageSize;
        var nextLast = done ? 0 : items[^1].RowNo;

        return new PageResult(items, nextLast, done);
    }

    public async Task<IReadOnlyList<PageItem>> FetchAllAsync(Guid resultId, CancellationToken ct)
    {
        // resultId は OpenQuery 登録された (sqlKey, parameters) を参照するために使用する。
        // まだ登録がない場合は固定 SQL（MEASURE_LOT_HISTORY_1PC_ALL）として扱う。
        var items = new List<PageItem>(capacity: 1024);

        // Resolve sqlKey + parameters
        var baseSqlKey = "MEASURE_LOT_HISTORY_1PC";
        IReadOnlyDictionary<string, string> parameters = new Dictionary<string, string>();
        if (_registry.TryGet(resultId, out var ctx))
        {
            if (!string.IsNullOrWhiteSpace(ctx.SqlKey)) baseSqlKey = ctx.SqlKey;
            parameters = ctx.Parameters;
        }

        // "xxx_ALL" がカタログに存在すればそれを使う。なければ既定。
        var sqlKeyAll = baseSqlKey + "_ALL";
        string sqlTemplate;
        try
        {
            sqlTemplate = _catalog.GetFetchPageSql(sqlKeyAll);
        }
        catch
        {
            sqlKeyAll = "MEASURE_LOT_HISTORY_1PC_ALL";
            sqlTemplate = _catalog.GetFetchPageSql(sqlKeyAll);
        }

        // Build WHERE from allowed parameters
        var allowed = new HashSet<string>(_catalog.GetAllowedParameters(baseSqlKey), StringComparer.OrdinalIgnoreCase);
        var conds = new List<string>();
        foreach (var kv in parameters)
        {
            if (!allowed.Contains(kv.Key)) continue;
            var col = MapParamToColumn(kv.Key);
            if (col is null) continue;
            conds.Add($"AND {col} = {_dialect.Param(kv.Key)}");
        }

        var whereClause = conds.Count == 0 ? "" : string.Join("\n        ", conds);
        var sqlText = sqlTemplate.Replace("{{where}}", whereClause);


        await using var conn = await _factory.OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sqlText;

        // SQL本文に含まれるパラメータだけ bind
        void AddIfUsed(string name, object? value)
        {
            var token = _dialect.Param(name);
            if (SqlUsesParam(cmd.CommandText, token))
            {
                _dialect.AddParameter(cmd, name, value);
            }
        }

        // Bind query parameters
        foreach (var kv in parameters)
        {
            if (!allowed.Contains(kv.Key)) continue;

            object? val = kv.Value;

            // Minimal type normalization
            if (kv.Key.Equals("partitionMKey", StringComparison.OrdinalIgnoreCase)
                && long.TryParse(kv.Value, out var l))
            {
                val = l;
            }

            // DateTime parameters (screen sends "YYYY/MM/DD HH:MI:SS")
            if ((kv.Key.Equals("startTime", StringComparison.OrdinalIgnoreCase)
                 || kv.Key.Equals("endTime", StringComparison.OrdinalIgnoreCase))
                && DateTime.TryParseExact(
                    kv.Value,
                    "yyyy/MM/dd HH:mm:ss",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out var dt))
            {
                val = dt;
            }

            AddIfUsed(kv.Key, val);
        }

        // === Debug: output SQL + parameters ===
        try
        {
            Directory.CreateDirectory(_dataDir);
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"{DateTime.Now:O} EXEC FetchAll resultId={resultId} sqlKey={sqlKeyAll}");
            sb.AppendLine("SQL:");
            sb.AppendLine(cmd.CommandText);
            sb.AppendLine("PARAMETERS:");
            foreach (DbParameter p in cmd.Parameters)
            {
                var v = p.Value is null || p.Value == DBNull.Value ? "null" : p.Value.ToString();
                sb.AppendLine($"  {p.ParameterName} = {v}");
            }
            sb.AppendLine("----");
            File.AppendAllText(Path.Combine(_dataDir, "dbaccess_sql_exec.log"), sb.ToString());
        }
        catch
        {
            // ignore logging failures
        }

        // SequentialAccess 読み出し（大きい結果セットを想定）
        await using var reader = await cmd.ExecuteReaderAsync(
            System.Data.CommandBehavior.SequentialAccess,
            ct);

        while (await reader.ReadAsync(ct))
        {
            var rowNo = Convert.ToInt32(reader.GetValue(0));

            string valueJson;
            if (reader.FieldCount >= 2 && reader.FieldCount <= 3)
            {
                // Vertica catalog returns (RowNo, Value)
                valueJson = reader.IsDBNull(1) ? "{}" : reader.GetString(1);
            }
            else
            {
                // SQLServer catalog returns the full column list
                var payload = new
                {
                    measureDataKind = reader.IsDBNull(1) ? null : reader.GetString(1),
                    testProcessName = reader.IsDBNull(2) ? null : reader.GetString(2),
                    lotName = reader.IsDBNull(3) ? null : reader.GetString(3),
                    measureStartDate = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                    measureEndDate = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5),
                    partitionMKey = reader.IsDBNull(6) ? (int?)null : Convert.ToInt32(reader.GetValue(6)),
                    program = reader.IsDBNull(7) ? null : reader.GetString(7),
                    equipmentId = reader.IsDBNull(8) ? null : reader.GetString(8),
                    operatorName = reader.IsDBNull(9) ? null : reader.GetString(9),
                    deviceType = reader.IsDBNull(10) ? null : reader.GetString(10),
                    tecKind = reader.IsDBNull(11) ? null : reader.GetString(11),
                    devtypeInfo = reader.IsDBNull(12) ? null : reader.GetString(12),
                    chipCount = reader.IsDBNull(13) ? (int?)null : Convert.ToInt32(reader.GetValue(13)),
                    msExist = reader.IsDBNull(14) ? null : reader.GetString(14),
                    markLotName = reader.IsDBNull(15) ? null : reader.GetString(15),
                    nobinFlag = reader.IsDBNull(16) ? null : reader.GetString(16),
                    stDatetime = reader.IsDBNull(17) ? (DateTime?)null : reader.GetDateTime(17),
                };

                valueJson = JsonSerializer.Serialize(payload);
            }

            items.Add(new PageItem(rowNo, valueJson));
        }

        return items;
    }

    private static string? MapParamToColumn(string key)
        => key.ToLowerInvariant() switch
        {
            "lotname" => "LOT_NAME",
            "equipmentid" => "EQUIPMENT_ID",
            "testprocessname" => "TEST_PROCESS_NAME",
            "measuredatakind" => "MEASURE_DATA_KIND",
            "partitionmkey" => "PARTITION_M_KEY",
            _ => null
        };
}
