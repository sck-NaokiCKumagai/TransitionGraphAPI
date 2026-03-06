using System.Diagnostics;
using System.Runtime.InteropServices;
using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
using TransitionGraph.V1;
using TransitionGraph.PortSetting;

/// <summary>
/// WorkerManager-Workerマネージャ
/// 処理概略：Workerの管理(Manager)を行う
/// </summary>
public sealed class WorkerManager
{
    private readonly List<Process> _procs = new();

    // 親プロセスが落ちたら子プロセスも道連れにする（Windows Job Object）
    private readonly JobObject? _job = JobObject.TryCreate();

    // 正常終了シーケンス中は Exited を無視
    private volatile bool _shuttingDown;

    public string DataEditAddress { get; private set; } = PortSettingStore.Current.WorkerDataEditAddress;
    public string DBAccessAddress { get; private set; } = PortSettingStore.Current.WorkerDbAccessAddress;
    public string OutputAddress { get; private set; } = PortSettingStore.Current.WorkerOutputAddress;
    public string AlignmentAddress { get; private set; } = PortSettingStore.Current.WorkerAlignmentAddress;

    /// <summary>
    /// StartAsync-Start（非同期）
    /// 処理概略：Asyncを開始する
    /// </summary>
    public Task StartAsync(IConfiguration cfg, string dataDir)
    {
        System.Diagnostics.Debug.WriteLine($"[INFO] StartAsync START");

        // Addresses
        DataEditAddress = cfg.GetValue("Workers:DataEditAddress", DataEditAddress);
        DBAccessAddress = cfg.GetValue("Workers:DBAccessAddress", DBAccessAddress);
        OutputAddress = cfg.GetValue("Workers:OutputAddress", OutputAddress);
        AlignmentAddress = cfg.GetValue("Workers:AlignmentAddress", AlignmentAddress);

        // Worker exe names/paths
        // - Publish layout: put worker exes next to host under folders like "dbaccess\TransitionGraphDBAccess.exe"
        // - VS Debug layout: automatically resolves to sibling project output under ...\TrasitionGraphDBAccess\bin\Debug\net8.0\TransitionGraphDBAccess.exe
        var dataEditExe = cfg.GetValue("Workers:DataEditExe", "TransitionGraphDataEdit.exe");
        var dbAccessExe = cfg.GetValue("Workers:DBAccessExe", "TransitionGraphDBAccess.exe");
        var outputExe = cfg.GetValue("Workers:OutputExe", "TransitionGraphOutput.exe");
        var alignmentExe = cfg.GetValue("Workers:AlignmentExe", "TransitionGraphAlignment.exe");

        // Ports
        var dePort = cfg.GetValue("Workers:DataEditPort", PortSettingStore.Current.WorkerDataEditPort);
        var dbPort = cfg.GetValue("Workers:DBAccessPort", PortSettingStore.Current.WorkerDbAccessPort);
        var outPort = cfg.GetValue("Workers:OutputPort", PortSettingStore.Current.WorkerOutputPort);
        var alPort = cfg.GetValue("Workers:AlignmentPort", PortSettingStore.Current.WorkerAlignmentPort);

        // Start DataEdit + DBAccess for API orchestration.
        // (Alignment/Output are not used in this phase)
        Spawn(dataEditExe, dePort, dataDir, "DataEdit", "TransitionGraphDataEdit", "TransitionGraphDataEdit");
        Spawn(dbAccessExe, dbPort, dataDir, "DBAccess", "TrasitionGraphDBAccess", "TransitionGraphDBAccess");
        //Spawn(outputExe, outPort, dataDir, "Output", "TransitionGraphOutput", "TransitionGraphOutput");
        Spawn(alignmentExe, alPort, dataDir, "Alignment", "TransitionGraphAlignment", "TransitionGraphAlignment");

        return Task.CompletedTask;
    }

    /// <summary>
    /// Spawn-Spawn
    /// 処理概略：Spawnを処理する
    /// </summary>
    private void Spawn(string exePathOrName, int port, string dataDir, string role, string devProjectDirName, string devExeBaseName)
    {
        var fullExe = ResolveWorkerExe(exePathOrName, devProjectDirName, devExeBaseName);

        if (fullExe is null)
        {
            System.Diagnostics.Debug.WriteLine($"[WARN] Worker exe not found, skip: role={role}, configured={exePathOrName}");
            return;
        }
        else
        {
            System.Diagnostics.Debug.WriteLine($"[INFO] Spawning {role}: {fullExe} (port={port}, dataDir={dataDir})");
        }

        /*****
        var psi = new ProcessStartInfo
        {
            FileName = fullExe,
            //Arguments = $"--port {port} --dataDir \"{dataDir}\"",
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory,
        };
        *****/
        var psi = new ProcessStartInfo
        {
            FileName = fullExe,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory,

            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = System.Text.Encoding.UTF8,
            StandardErrorEncoding = System.Text.Encoding.UTF8,
        };

        // 文字列連結で渡さない
        psi.ArgumentList.Add("--port");
        psi.ArgumentList.Add(port.ToString());
        psi.ArgumentList.Add("--dataDir");
        psi.ArgumentList.Add(dataDir);

        var p = new Process { StartInfo = psi, EnableRaisingEvents = true };

        /*****
        p.Start();
        *****/
        p.OutputDataReceived += (_, e) =>
        {
            if (!string.IsNullOrWhiteSpace(e.Data))
                Console.WriteLine($"[{role} STDOUT] {e.Data}");
        };

        p.ErrorDataReceived += (_, e) =>
        {
            if (!string.IsNullOrWhiteSpace(e.Data))
                Console.WriteLine($"[{role} STDERR] {e.Data}");
        };

        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();

        _procs.Add(p);

        // Job Object に紐付け（親が死んだら子も落とす）
        _job?.AddProcess(p);

        // だれか1つでも落ちたら全員落とす（フェイルファスト）
        p.Exited += (_, __) => OnWorkerExited(role, p);
    }

    /// <summary>
    /// OnWorkerExited-OnWorkerExited
    /// 処理概略：OnWorkerExitedを処理する
    /// </summary>
    private void OnWorkerExited(string role, Process p)
    {
        if (_shuttingDown) return;

        try
        {
            System.Diagnostics.Debug.WriteLine($"[ERROR] Worker exited: role={role} pid={p.Id} exitCode={(p.HasExited ? p.ExitCode : -1)}");
        }
        catch { }

        // 親プロセスを落とす → Job Object のハンドルが閉じられ、他の子プロセスも強制終了される
        Environment.Exit(1);
    }

    /// <summary>
    /// Resolve worker exe path.
    /// Priority:
    /// 1) Absolute path as-is
    /// 2) Relative to Host base directory (AppContext.BaseDirectory)
    /// 3) Dev layout: sibling project output under repo root
    /// </summary>
    private static string? ResolveWorkerExe(string exePathOrName, string devProjectDirName, string devExeBaseName)
    {
        if (string.IsNullOrWhiteSpace(exePathOrName))
            return null;

        // 1) absolute
        if (Path.IsPathRooted(exePathOrName))
            return File.Exists(exePathOrName) ? exePathOrName : null;

        // 2) relative to host base dir
        var hostBase = AppContext.BaseDirectory;
        var candidate1 = Path.GetFullPath(Path.Combine(hostBase, exePathOrName));
        if (File.Exists(candidate1)) return candidate1;

        // Also allow common publish subfolder layout: {base}\dbaccess\TransitionGraphDBAccess.exe
        // If exePathOrName is just a filename, try role folder names.
        if (Path.GetFileName(exePathOrName).Equals(exePathOrName, StringComparison.OrdinalIgnoreCase))
        {
            var candidate2 = Path.GetFullPath(Path.Combine(hostBase, "dbaccess", exePathOrName));
            if (File.Exists(candidate2)) return candidate2;

            var candidate3 = Path.GetFullPath(Path.Combine(hostBase, "dataedit", exePathOrName));
            if (File.Exists(candidate3)) return candidate3;

            var candidate4 = Path.GetFullPath(Path.Combine(hostBase, "output", exePathOrName));
            if (File.Exists(candidate4)) return candidate4;

            var candidate5 = Path.GetFullPath(Path.Combine(hostBase, "alignment", exePathOrName));
            if (File.Exists(candidate5)) return candidate5;
        }

        // 3) VS dev layout: find sln up the tree, then go one directory up as repo root
        var repoRoot = FindRepoRootFromHostBase(hostBase);
        if (repoRoot is null) return null;

        // Determine Debug/Release by probing
        var debugDir = Path.Combine(repoRoot, devProjectDirName, "bin", "Debug", "net8.0");
        var releaseDir = Path.Combine(repoRoot, devProjectDirName, "bin", "Release", "net8.0");
        var cfgDir = Directory.Exists(debugDir) ? debugDir : (Directory.Exists(releaseDir) ? releaseDir : null);
        if (cfgDir is null) return null;

        var exeName = exePathOrName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase)
            ? exePathOrName
            : devExeBaseName + ".exe";

        var candidateDev = Path.Combine(cfgDir, exeName);
        return File.Exists(candidateDev) ? candidateDev : null;
    }

    /// <summary>
    /// FindRepoRootFromHostBase-検索RepoRoot←HostBase
    /// 処理概略：RepoRootFromHostBaseを検索する
    /// </summary>
    private static string? FindRepoRootFromHostBase(string hostBase)
    {
        var dir = new DirectoryInfo(hostBase);
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            // If .sln exists here, repo root is parent of this folder
            var slns = dir.GetFiles("*.sln", SearchOption.TopDirectoryOnly);
            if (slns.Length > 0)
            {
                return dir.Parent?.FullName ?? dir.FullName;
            }

            // Also accept "TransitionGraphAPI.sln" living under TransitionGraphAPI\ (project folder),
            // in that case repo root is parent of that folder.
            var transitionGraphApiDir = new DirectoryInfo(Path.Combine(dir.FullName, "TransitionGraphAPI"));
            if (transitionGraphApiDir.Exists)
            {
                var sln2 = transitionGraphApiDir.GetFiles("*.sln", SearchOption.TopDirectoryOnly);
                if (sln2.Length > 0)
                {
                    return dir.FullName;
                }
            }

            dir = dir.Parent;
        }
        return null;
    }

    /// <summary>
    /// ShutdownAsync-Shutdown（非同期）
    /// 処理概略：ShutdownAsyncを処理する
    /// </summary>
    public async Task ShutdownAsync()
    {
        _shuttingDown = true;
        // Best-effort gRPC shutdown, then kill if needed.
        await TryShutdownOne(DataEditAddress);
        await TryShutdownOne(DBAccessAddress);
        await TryShutdownOne(OutputAddress);
        await TryShutdownOne(AlignmentAddress);

        foreach (var p in _procs)
        {
            try
            {
                if (!p.HasExited)
                {
                    if (!p.WaitForExit(3000))
                        p.Kill(entireProcessTree: true);
                }
            }
            catch { /* ignore */ }
        }
    }

    /// <summary>
    /// TryShutdownOne-試行ShutdownOne
    /// 処理概略：ShutdownOneを試行する
    /// </summary>
    private static async Task TryShutdownOne(string addr)
    {
        try
        {
            using var ch = GrpcChannel.ForAddress(addr);
            var ctl = new Control.ControlClient(ch);
            await ctl.ShutdownAsync(new Empty());
        }
        catch { /* ignore */ }
    }
}


/// <summary>
/// JobObject-JobObject
/// 処理概略：JobObjectに関する処理をまとめる
/// </summary>
internal sealed class JobObject : IDisposable
{
    private IntPtr _hJob;

    /// <summary>
    /// JobObject-JobObject
    /// 処理概略：JobObjectの初期化を行う
    /// </summary>
    private JobObject(IntPtr hJob)
    {
        _hJob = hJob;
    }

    /// <summary>
    /// TryCreate-試行作成
    /// 処理概略：Createを試行する
    /// </summary>
    public static JobObject? TryCreate()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return null;

        var h = CreateJobObject(IntPtr.Zero, null);
        if (h == IntPtr.Zero) return null;

        var info = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT.KILL_ON_JOB_CLOSE;

        var length = Marshal.SizeOf<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>();
        var ptr = Marshal.AllocHGlobal(length);
        try
        {
            Marshal.StructureToPtr(info, ptr, fDeleteOld: false);
            if (!SetInformationJobObject(h, JOBOBJECTINFOCLASS.JobObjectExtendedLimitInformation, ptr, (uint)length))
            {
                CloseHandle(h);
                return null;
            }
        }
        finally
        {
            Marshal.FreeHGlobal(ptr);
        }

        return new JobObject(h);
    }

    /// <summary>
    /// AddProcess-AddProcess
    /// 処理概略：Processを追加する
    /// </summary>
    public void AddProcess(Process p)
    {
        if (_hJob == IntPtr.Zero) return;
        try
        {
            var hProc = p.Handle; // ensures handle is created
            AssignProcessToJobObject(_hJob, hProc);
        }
        catch
        {
            // ignore
        }
    }

    /// <summary>
    /// Dispose-Dispose
    /// 処理概略：処理を破棄する
    /// </summary>
    public void Dispose()
    {
        var h = _hJob;
        _hJob = IntPtr.Zero;
        if (h != IntPtr.Zero) CloseHandle(h);
    }

    /// <summary>
    /// JOBOBJECTINFOCLASS-JOBOBJECTINFOCLASS
    /// 処理概略：JOBOBJECTINFOCLASSに関する処理をまとめる
    /// </summary>
    private enum JOBOBJECTINFOCLASS
    {
        JobObjectExtendedLimitInformation = 9
    }

    /// <summary>
    /// JOB_OBJECT_LIMIT-JOBOBJECTLIMIT
    /// 処理概略：JOBOBJECTLIMITに関する処理をまとめる
    /// </summary>
    [Flags]
    private enum JOB_OBJECT_LIMIT : uint
    {
        KILL_ON_JOB_CLOSE = 0x00002000
    }

    /// <summary>
    /// IO_COUNTERS-IOCOUNTERS
    /// 処理概略：IOCOUNTERSに関する処理をまとめる
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    /// <summary>
    /// JOBOBJECT_BASIC_LIMIT_INFORMATION-JOBOBJECTBASICLIMITINFORMATION
    /// 処理概略：JOBOBJECTBASICLIMITINFORMATIONに関する処理をまとめる
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public JOB_OBJECT_LIMIT LimitFlags;
        public UIntPtr MinimumWorkingSetSize;
        public UIntPtr MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public IntPtr Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

    /// <summary>
    /// JOBOBJECT_EXTENDED_LIMIT_INFORMATION-JOBOBJECTEXTENDEDLIMITINFORMATION
    /// 処理概略：JOBOBJECTEXTENDEDLIMITINFORMATIONに関する処理をまとめる
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
        public IO_COUNTERS IoInfo;
        public UIntPtr ProcessMemoryLimit;
        public UIntPtr JobMemoryLimit;
        public UIntPtr PeakProcessMemoryUsed;
        public UIntPtr PeakJobMemoryUsed;
    }

    /// <summary>
    /// CreateJobObject-作成JobObject
    /// 処理概略：JobObjectを作成する
    /// </summary>
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string? lpName);

    /// <summary>
    /// SetInformationJobObject-SetInformationJobObject
    /// 処理概略：InformationJobObjectを設定する
    /// </summary>
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetInformationJobObject(IntPtr hJob, JOBOBJECTINFOCLASS infoClass, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength);

    /// <summary>
    /// AssignProcessToJobObject-AssignProcess→JobObject
    /// 処理概略：AssignProcessToJobObjectを処理する
    /// </summary>
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProcess);

    /// <summary>
    /// CloseHandle-CloseHandle
    /// 処理概略：Handleを閉じるする
    /// </summary>
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr hObject);
}