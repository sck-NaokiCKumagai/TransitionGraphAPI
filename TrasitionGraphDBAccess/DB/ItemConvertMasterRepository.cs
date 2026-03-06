using System.Data.Common;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace TransitionGraph.Db;

public sealed record ItemConvertRowModel(string EquipmentId, long ConvertOrder, string ConvertMethod);

public interface IItemConvertMasterRepository
{
    Task<int> DumpAsync(int maxRows, CancellationToken ct);
    Task<(int RowsFetched, List<ItemConvertRowModel> Sample)> ProbeAsync(int maxRows, int sampleRows, CancellationToken ct);
}

public sealed class ItemConvertMasterRepository : IItemConvertMasterRepository
{
    private readonly IDbConnectionFactory _factory;
    private readonly DatabaseOptions _opt;
    private readonly ILogger<ItemConvertMasterRepository> _logger;
    private readonly DataDir _dir; // ★追加

    public ItemConvertMasterRepository(
        IDbConnectionFactory factory,
        Microsoft.Extensions.Options.IOptions<DatabaseOptions> opt,
        ILogger<ItemConvertMasterRepository> logger,
        DataDir dir) // ★追加
    {
        _factory = factory;
        _opt = opt.Value;
        _logger = logger;
        _dir = dir;
    }

    public async Task<int> DumpAsync(int maxRows, CancellationToken ct)
    {
        Directory.CreateDirectory(_dir.Path);
        var logPath = Path.Combine(_dir.Path, "dbprobe_item_convert_master.log");

        void FileLog(string msg)
        {
            File.AppendAllText(logPath, $"{DateTime.Now:O} {msg}\n");
        }

        FileLog($"ENTER DumpAsync Provider={_factory.ProviderType} MaxRows={maxRows}");

        try
        {
            FileLog("BEFORE OpenConnectionAsync");
            await using var conn = await _factory.OpenConnectionAsync(ct);
            FileLog("AFTER OpenConnectionAsync");

            await using var cmd = conn.CreateCommand();
            cmd.CommandTimeout = _opt.CommandTimeoutSeconds;

            if (_factory.ProviderType == DbProviderType.Vertica)
            {
                cmd.CommandText = @"
SELECT EQUIPMENTID, CONVERTORDER, CONVERTMETHOD
FROM nikuser.ITEM_CONVERT_MASTER
ORDER BY EQUIPMENTID, CONVERTORDER
LIMIT :n;
".Trim();

                var p = cmd.CreateParameter();
                p.ParameterName = "n";
                p.Value = maxRows;
                cmd.Parameters.Add(p);
            }
            else
            {
                cmd.CommandText = @"
SELECT TOP (@n) EQUIPMENTID, CONVERTORDER, CONVERTMETHOD
FROM [nikuser].[ITEM_CONVERT_MASTER]
ORDER BY EQUIPMENTID, CONVERTORDER;
".Trim();

                var p = cmd.CreateParameter();
                p.ParameterName = "@n";
                p.Value = maxRows;
                cmd.Parameters.Add(p);
            }

            _logger.LogInformation("Dump ITEM_CONVERT_MASTER start. Provider={Provider}, MaxRows={MaxRows}",
                _factory.ProviderType, maxRows);
            Debug.WriteLine($"Dump start Provider={_factory.ProviderType} MaxRows={maxRows}");
            FileLog("BEFORE ExecuteReaderAsync");

            int count = 0;
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            FileLog("AFTER ExecuteReaderAsync");

            while (await reader.ReadAsync(ct))
            {
                var equipmentId = reader.IsDBNull(0) ? "" : reader.GetString(0);

                long convertOrder = 0;
                if (!reader.IsDBNull(1))
                {
                    var v = reader.GetValue(1);
                    convertOrder = v switch
                    {
                        long l => l,
                        int i => i,
                        decimal d => (long)d,
                        _ => Convert.ToInt64(v)
                    };
                }

                var convertMethod = reader.IsDBNull(2) ? "" : reader.GetString(2);

                // 全行をファイルに書くと膨らむので、最初の数行だけ残す（必要なら増やしてOK）
                if (count < 20)
                    FileLog($"ROW {count + 1}: EQUIPMENTID={equipmentId}, CONVERTORDER={convertOrder}, CONVERTMETHOD={convertMethod}");

                count++;
            }

            FileLog($"END DumpAsync Rows={count}");
            _logger.LogInformation("Dump ITEM_CONVERT_MASTER end. Rows={Count}", count);
            Debug.WriteLine($"Dump end Rows={count}");

            return count;
        }
        catch (Exception ex)
        {
            FileLog($"ERROR {ex}");
            _logger.LogError(ex, "DumpAsync failed.");
            Debug.WriteLine(ex.ToString());
            throw;
        }
    }

    public async Task<(int RowsFetched, List<ItemConvertRowModel> Sample)> ProbeAsync(int maxRows, int sampleRows, CancellationToken ct)
    {
        Directory.CreateDirectory(_dir.Path);
        var logPath = Path.Combine(_dir.Path, "dbprobe_item_convert_master.log");

        void FileLog(string msg)
        {
            File.AppendAllText(logPath, $"{DateTime.Now:O} {msg}");
        }

        FileLog($"ENTER ProbeAsync Provider={_factory.ProviderType} MaxRows={maxRows} SampleRows={sampleRows}");

        try
        {
            FileLog("BEFORE OpenConnectionAsync");
            await using var conn = await _factory.OpenConnectionAsync(ct);
            FileLog("AFTER OpenConnectionAsync");

            await using var cmd = conn.CreateCommand();
            cmd.CommandTimeout = _opt.CommandTimeoutSeconds;

            if (_factory.ProviderType == DbProviderType.Vertica)
            {
                cmd.CommandText = @"
SELECT EQUIPMENTID, CONVERTORDER, CONVERTMETHOD
FROM nikuser.ITEM_CONVERT_MASTER
ORDER BY EQUIPMENTID, CONVERTORDER
LIMIT :n;
".Trim();

                var p = cmd.CreateParameter();
                p.ParameterName = "n";
                p.Value = maxRows;
                cmd.Parameters.Add(p);
            }
            else
            {
                cmd.CommandText = @"
SELECT TOP (@n) EQUIPMENTID, CONVERTORDER, CONVERTMETHOD
FROM [nikuser].[ITEM_CONVERT_MASTER]
ORDER BY EQUIPMENTID, CONVERTORDER;
".Trim();

                var p = cmd.CreateParameter();
                p.ParameterName = "@n";
                p.Value = maxRows;
                cmd.Parameters.Add(p);
            }

            FileLog("BEFORE ExecuteReaderAsync");
            int count = 0;
            var sample = new List<ItemConvertRowModel>(Math.Max(0, sampleRows));

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            FileLog("AFTER ExecuteReaderAsync");

            while (await reader.ReadAsync(ct))
            {
                var equipmentId = reader.IsDBNull(0) ? "" : reader.GetString(0);

                long convertOrder = 0;
                if (!reader.IsDBNull(1))
                {
                    var v = reader.GetValue(1);
                    convertOrder = v switch
                    {
                        long l => l,
                        int i => i,
                        decimal d => (long)d,
                        _ => Convert.ToInt64(v)
                    };
                }

                var convertMethod = reader.IsDBNull(2) ? "" : reader.GetString(2);

                if (count < sampleRows)
                {
                    sample.Add(new ItemConvertRowModel(equipmentId, convertOrder, convertMethod));
                    FileLog($"ROW {count + 1}: EQUIPMENTID={equipmentId}, CONVERTORDER={convertOrder}, CONVERTMETHOD={convertMethod}");
                }

                count++;
            }

            FileLog($"END ProbeAsync Rows={count}");
            return (count, sample);
        }
        catch (Exception ex)
        {
            FileLog($"ERROR {ex}");
            throw;
        }
    }
}
