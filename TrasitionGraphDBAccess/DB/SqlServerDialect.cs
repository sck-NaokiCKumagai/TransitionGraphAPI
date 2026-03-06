using System.Data.Common;

namespace TransitionGraph.Db;

public sealed class SqlServerDialect : ISqlDialect
{
    public string Param(string name) => "@" + name;
    public string ParameterName(string name) => "@" + name;

    // Fetch fixed table MEASURE_LOT_HISTORY_1PC with stable ordering and RowNo paging (no OFFSET).
    // NOTE: ResultId is kept for API compatibility but not used in this step.
    public string BuildFetchPageSql(string? whereClause = null) =>
$@"
WITH src AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                EQUIPMENT_ID,
                ST_DATETIME,
                LOT_NAME,
                PARTITION_M_KEY
        ) AS RowNo,
        MEASURE_DATA_KIND,
        TEST_PROCESS_NAME,
        LOT_NAME,
        MEASURE_START_DATE,
        MEASURE_END_DATE,
        PARTITION_M_KEY,
        [PROGRAM] AS PROGRAM,
        EQUIPMENT_ID,
        OPERATOR_NAME,
        DEVICE_TYPE,
        TEC_KIND,
        DEVTYPE_INFO,
        CHIP_COUNT,
        MS_EXIST,
        MARK_LOT_NAME,
        NOBIN_FLAG,
        ST_DATETIME
    FROM nikuser.MEASURE_LOT_HISTORY_1PC
    WHERE 1=1
        {(string.IsNullOrWhiteSpace(whereClause) ? "" : whereClause)}
)
SELECT TOP (@pageSize)
    RowNo,
    MEASURE_DATA_KIND,
    TEST_PROCESS_NAME,
    LOT_NAME,
    MEASURE_START_DATE,
    MEASURE_END_DATE,
    PARTITION_M_KEY,
    PROGRAM,
    EQUIPMENT_ID,
    OPERATOR_NAME,
    DEVICE_TYPE,
    TEC_KIND,
    DEVTYPE_INFO,
    CHIP_COUNT,
    MS_EXIST,
    MARK_LOT_NAME,
    NOBIN_FLAG,
    ST_DATETIME
FROM src
WHERE RowNo > @lastRowNo
ORDER BY RowNo;
".Trim();

    public DbParameter AddParameter(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = ParameterName(name);
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
        return p;
    }
}
