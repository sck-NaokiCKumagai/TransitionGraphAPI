using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;
using TransitionGraph.LogOutPut.Internal;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace TransitionGraph.LogOutPut;

/// <summary>
/// LogKind-ログKind
/// 処理概略：LogKindに関する処理をまとめる
/// </summary>
public enum LogKind
{
    DEBUG,
    INFO,
    WARN,
    ERROR
}

/// <summary>
/// MessageOut-メッセージ出力処理
/// 処理概略：MessageOutに関する処理をまとめる
/// </summary>
public static class MessageOut
{
    private static readonly object _lock = new();
    private static bool _initialized;

    private static readonly ILog _log = LogManager.GetLogger("TransitionGraphLogger");

    // 定期削除用
    private static DateTime _lastCleanup = DateTime.MinValue;
    private static readonly TimeSpan _cleanupInterval = TimeSpan.FromHours(6); // 好きに変更OK
    private static string _logDir = @".\logs";

    // ------------------------------
    // 呼び出し側が使うAPI（基本）
    // ------------------------------

    // INFO固定（既存互換）
    /// <summary>
    /// MessageOutPut-メッセージ出力処理
    /// 処理概略：MessageOutPutを処理する
    /// </summary>
    public static void MessageOutPut(
        string message,
        [CallerFilePath] string file = "",
        [CallerMemberName] string member = "",
        [CallerLineNumber] int line = 0)
    {
        Write(LogKind.INFO, message, null, file, member, line);
    }

    /// <summary>
    /// MessageOutPut2-メッセージ出力処理2
    /// 処理概略：MessageOutPut2を処理する
    /// </summary>
    public static void MessageOutPut2(
        string message,
        [CallerMemberName] string member = "")
    {
        Write(LogKind.INFO, message, null, null, member, 0);
    }

    // 種別指定
    /// <summary>
    /// MessageOutPut-メッセージ出力処理(種別指定)
    /// 処理概略：MessageOutPutを処理する
    /// </summary>
    public static void MessageOutPut(
        string message,
        LogKind kind,
        [CallerFilePath] string file = "",
        [CallerMemberName] string member = "",
        [CallerLineNumber] int line = 0)
    {
        Write(kind, message, null, file, member, line);
    }

    // Exception付き
    /// <summary>
    /// MessageOutPut-メッセージ出力処理(Exception付)
    /// 処理概略：MessageOutPutを処理する
    /// </summary>
    public static void MessageOutPut(
        string message,
        LogKind kind,
        Exception ex,
        [CallerFilePath] string file = "",
        [CallerMemberName] string member = "",
        [CallerLineNumber] int line = 0)
    {
        Write(kind, message, ex, file, member, line);
    }

    // ------------------------------
    // 内部処理
    // ------------------------------

    /// <summary>
    /// Write-書込
    /// 処理概略：処理を書き込みする
    /// </summary>
    private static void Write(
        LogKind kind,
        string message,
        Exception? ex,
        string callerFilePath,
        string callerMemberName,
        int callerLineNumber)
    {
        EnsureInitialized();

        // 定期削除（ログ出力時についでに）
        MaybeCleanup();

        // 呼び出し元を埋める（PatternLayoutで表示する）
        var callerClass = Path.GetFileNameWithoutExtension(callerFilePath);
        ThreadContext.Properties["CallerClass"] = callerClass;
        ThreadContext.Properties["CallerMember"] = callerMemberName;
        ThreadContext.Properties["CallerLine"] = callerLineNumber;

        try
        {
            switch (kind)
            {
                case LogKind.DEBUG:
                    if (ex is null) _log.Debug(message);
                    else _log.Debug(message, ex);
                    break;

                case LogKind.INFO:
                    if (ex is null) _log.Info(message);
                    else _log.Info(message, ex);
                    break;

                case LogKind.WARN:
                    if (ex is null) _log.Warn(message);
                    else _log.Warn(message, ex);
                    break;

                case LogKind.ERROR:
                    if (ex is null) _log.Error(message);
                    else _log.Error(message, ex);
                    break;

                default:
                    if (ex is null) _log.Info(message);
                    else _log.Info(message, ex);
                    break;
            }
        }
        finally
        {
            // 必要なら掃除（汚染防止）
            ThreadContext.Properties.Remove("CallerClass");
            ThreadContext.Properties.Remove("CallerMember");
            ThreadContext.Properties.Remove("CallerLine");
        }
    }

    /// <summary>
    /// EnsureInitialized-EnsureInitialized
    /// 処理概略：Initializedを保証する
    /// </summary>
    private static void EnsureInitialized()
    {
        if (_initialized) return;

        lock (_lock)
        {
            if (_initialized) return;

            var props = PropertiesLoader.Load(PropertiesLoader.GetPropertiesPath());

            _logDir = Environment.ExpandEnvironmentVariables(
                props.GetValueOrDefault("LogDirectory", @".\logs")
            );

            int maxSizeMb = PropertiesLoader.ParseInt(
                props.GetValueOrDefault("MaxFileSizeMB", "10"),
                defaultValue: 10
            );

            string minLevel = props.GetValueOrDefault("MinLogLevel", "DEBUG");

            Directory.CreateDirectory(_logDir);

            ConfigureLog4Net(_logDir, maxSizeMb, minLevel);

            // 起動時にも削除
            LogCleanup.DeleteOlderThanDays(_logDir, keepDays: 7);
            _lastCleanup = DateTime.Now;

            _initialized = true;
        }
    }

    /// <summary>
    /// MaybeCleanup-MaybeCleanup
    /// 処理概略：MaybeCleanupを処理する
    /// </summary>
    private static void MaybeCleanup()
    {
        var now = DateTime.Now;
        if (now - _lastCleanup < _cleanupInterval) return;

        lock (_lock)
        {
            // 二重チェック
            now = DateTime.Now;
            if (now - _lastCleanup < _cleanupInterval) return;

            LogCleanup.DeleteOlderThanDays(_logDir, keepDays: 7);
            _lastCleanup = now;
        }
    }

    /// <summary>
    /// ConfigureLog4Net-Configureログ4Net
    /// 処理概略：Log4Netを設定する
    /// </summary>
    private static void ConfigureLog4Net(string logDir, int maxSizeMb, string minLevelStr)
    {
        var hierarchy = (Hierarchy)LogManager.GetRepository(Assembly.GetExecutingAssembly());
        hierarchy.Root.RemoveAllAppenders();

        // 年月日時分秒 [ログレベル] [スレッドID] [クラス.メソッド:行番号]：メッセージ
        var layout = new PatternLayout
        {
            ConversionPattern =
                "%date{yyyy/MM/dd HH:mm:ss} [%level] [T:%thread] " +
                "[%property{CallerClass}.%property{CallerMember}:%property{CallerLine}]：" +
                "%message%newline%exception"
        };
        layout.ActivateOptions();

        var appender = new RollingFileAppender
        {
            Name = "TransitionGraphRollingAppender",

            // ファイル名：TransitionGraph_YYYYMMDD.log
            File = Path.Combine(logDir, "TransitionGraph_"),
            DatePattern = "yyyyMMdd'.log'",
            StaticLogFileName = false,
            PreserveLogFileNameExtension = true,

            AppendToFile = true,

            // 日付＋サイズ ロール
            RollingStyle = RollingFileAppender.RollingMode.Composite,

            // サイズ超えで .1 .2 ...
            MaximumFileSize = $"{maxSizeMb}MB",
            MaxSizeRollBackups = 999,

            LockingModel = new FileAppender.MinimalLock(),
            Layout = layout
        };

        appender.ActivateOptions();
        hierarchy.Root.AddAppender(appender);

        hierarchy.Root.Level = ParseLevel(minLevelStr);
        hierarchy.Configured = true;
    }

    /// <summary>
    /// ParseLevel-解析Level
    /// 処理概略：Levelを解析する
    /// </summary>
    private static Level ParseLevel(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return Level.Debug;

        return s.Trim().ToUpperInvariant() switch
        {
            "ERROR" => Level.Error,
            "WARN" => Level.Warn,
            "INFO" => Level.Info,
            "DEBUG" => Level.Debug,
            _ => Level.Debug
        };
    }
}