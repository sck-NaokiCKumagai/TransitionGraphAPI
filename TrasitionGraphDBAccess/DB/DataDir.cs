namespace TransitionGraph.Db;

/// <summary>
/// A simple wrapper to pass the "data" directory (log output, temp files) via DI.
/// </summary>
public sealed class DataDir
{
    public string Path { get; }

    public DataDir(string path) => Path = path;
}
