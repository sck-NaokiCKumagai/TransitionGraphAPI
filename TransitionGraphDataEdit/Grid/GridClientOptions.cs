namespace TransitionGraphDataEdit.Grid;

public sealed class GridClientOptions
{
    /// <summary>
    /// True: use Dummy DLL (CSV). False: use real environment DLL.
    /// </summary>
    public bool UseDummy { get; set; } = true;

    /// <summary>
    /// Directory where dummy CSV files are located.
    /// </summary>
    public string DummyCsvDir { get; set; } = "DummyDLL/CSV";
}
