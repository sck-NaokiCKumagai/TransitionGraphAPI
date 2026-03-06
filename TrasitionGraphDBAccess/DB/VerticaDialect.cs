using System.Data.Common;

namespace TransitionGraph.Db;

public sealed class VerticaDialect : ISqlDialect
{
    // Vertica ADO.NET の例では SQL中は :name、パラメータ名は "name"（コロン無し）で渡すのが定番です :contentReference[oaicite:3]{index=3}
    public string Param(string name) => ":" + name;
    public string ParameterName(string name) => name;

    // Fixed SQL (conditions will be added later).
    // NOTE: :resultId is currently ignored to keep the existing gRPC signature.
    /*****/
    public string BuildFetchPageSql(string? whereClause = null) =>
@"
SELECT t.RowNo,
       t.Value
FROM (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                PARTITION_M_KEY,
                MEASURE_START_DATE,
                LOT_NAME,
                TEST_PROCESS_NAME,
                MEASURE_DATA_KIND,
                EQUIPMENT_ID,
                ST_DATETIME
        ) AS RowNo,
        (
            COALESCE(MEASURE_DATA_KIND, '') || '|' ||
            COALESCE(TEST_PROCESS_NAME, '') || '|' ||
            COALESCE(LOT_NAME, '') || '|' ||
            COALESCE(TO_CHAR(MEASURE_START_DATE, 'YYYY-MM-DD""T""HH24:MI:SS'), '') || '|' ||
            COALESCE(TO_CHAR(MEASURE_END_DATE, 'YYYY-MM-DD""T""HH24:MI:SS'), '') || '|' ||
            COALESCE(CAST(PARTITION_M_KEY AS VARCHAR), '') || '|' ||
            COALESCE(PROGRAM, '') || '|' ||
            COALESCE(EQUIPMENT_ID, '') || '|' ||
            COALESCE(OPERATOR_NAME, '') || '|' ||
            COALESCE(DEVICE_TYPE, '') || '|' ||
            COALESCE(TEC_KIND, '') || '|' ||
            COALESCE(DEVTYPE_INFO, '') || '|' ||
            COALESCE(CAST(CHIP_COUNT AS VARCHAR), '') || '|' ||
            COALESCE(MS_EXIST, '') || '|' ||
            COALESCE(MARK_LOT_NAME, '') || '|' ||
            COALESCE(NOBIN_FLAG, '') || '|' ||
            COALESCE(TO_CHAR(ST_DATETIME, 'YYYY-MM-DD""T""HH24:MI:SS'), '')
        ) AS Value
    FROM nikuser.MEASURE_LOT_HISTORY_1PC
    WHERE 1=1
        {(string.IsNullOrWhiteSpace(whereClause) ? "" : whereClause)}
) t
WHERE t.RowNo > :lastRowNo
ORDER BY t.RowNo
LIMIT :pageSize;
".Trim();
    /****/

    public DbParameter AddParameter(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = ParameterName(name);
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
        return p;
    }
}
