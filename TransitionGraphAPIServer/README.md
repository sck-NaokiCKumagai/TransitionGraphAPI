# TransitionGraphAPIServer (Wrapper Windows Service)

This project runs `TransitionGraphAPI.exe` as a child process and manages lifecycle:
- Start: launches TransitionGraphAPI and waits for /health.
- Stop: POST /internal/shutdown then waits; if still alive, kills.
- Crash: if TransitionGraphAPI exits unexpectedly, the wrapper process exits with code 1 (so SCM "Recovery" can restart it).

It also uses a Windows Job Object with `KILL_ON_JOB_CLOSE` so if the service process is terminated, the child is also terminated.

## Config
Edit `appsettings.json` (Wrapper section):
- TargetExeRelativePath: path from wrapper's base directory to TransitionGraphAPI.exe
- ApiBaseUrl, ShutdownPath, HealthPath
- StartupWaitSeconds, ShutdownWaitSeconds
