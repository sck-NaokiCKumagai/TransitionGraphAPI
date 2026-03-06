using System.Runtime.InteropServices;

internal sealed class JobObject : IDisposable
{
    private IntPtr _hJob;

    public JobObject()
    {
        _hJob = CreateJobObject(IntPtr.Zero, null);
        if (_hJob == IntPtr.Zero)
            throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());

        // KILL_ON_JOB_CLOSE を設定
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION info = new();
        info.BasicLimitInformation.LimitFlags = JOBOBJECT_LIMIT_FLAGS.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        int length = Marshal.SizeOf<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>();
        IntPtr ptr = Marshal.AllocHGlobal(length);
        try
        {
            Marshal.StructureToPtr(info, ptr, false);
            if (!SetInformationJobObject(_hJob, JOBOBJECTINFOCLASS.JobObjectExtendedLimitInformation, ptr, (uint)length))
                throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
        }
        finally
        {
            Marshal.FreeHGlobal(ptr);
        }
    }

    public void AddProcess(IntPtr hProcess)
    {
        if (!AssignProcessToJobObject(_hJob, hProcess))
            throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
    }

    public void Dispose()
    {
        if (_hJob != IntPtr.Zero)
        {
            CloseHandle(_hJob);
            _hJob = IntPtr.Zero;
        }
        GC.SuppressFinalize(this);
    }

    ~JobObject() => Dispose();

    // P/Invoke
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateJobObject(IntPtr lpJobAttributes, string? lpName);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetInformationJobObject(IntPtr hJob, JOBOBJECTINFOCLASS infoClass, IntPtr lpJobObjectInfo, uint cbJobObjectInfoLength);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AssignProcessToJobObject(IntPtr hJob, IntPtr hProcess);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr hObject);

    private enum JOBOBJECTINFOCLASS
    {
        JobObjectExtendedLimitInformation = 9
    }

    [Flags]
    private enum JOBOBJECT_LIMIT_FLAGS : uint
    {
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public JOBOBJECT_LIMIT_FLAGS LimitFlags;
        public UIntPtr MinimumWorkingSetSize;
        public UIntPtr MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public long Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

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
}
