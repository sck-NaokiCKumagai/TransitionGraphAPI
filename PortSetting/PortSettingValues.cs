namespace TransitionGraph.PortSetting;

/// <summary>
/// Strongly typed port settings shared by APIServer / APIClient.
/// Values are loaded from PortSetting.properties (Java-style .properties file).
/// </summary>
public sealed record PortSettingValues
{
    /// <summary>
    /// APIServer (TransitionGraphAPI) listen port.
    /// </summary>
    public int ApiServerPort { get; init; } = 5000;

    /// <summary>
    /// APIClient (CTransitionGraphAPI) listen port.
    /// </summary>
    public int ApiClientPort { get; init; } = 5500;

    /// <summary>
    /// Output worker (CTransitionGraphOutput) listen port.
    /// </summary>
    public int OutputPort { get; init; } = 5203;

    // Worker ports (gRPC)
    public int WorkerDataEditPort { get; init; } = 5101;
    public int WorkerDbAccessPort { get; init; } = 5102;
    public int WorkerOutputPort { get; init; } = 5103;
    public int WorkerAlignmentPort { get; init; } = 5104;

    /// <summary>
    /// Scheme used for building URLs when only ports are specified.
    /// </summary>
    public string Scheme { get; init; } = "http";

    /// <summary>
    /// Host used for building loopback URLs when only ports are specified.
    /// </summary>
    public string Host { get; init; } = "127.0.0.1";

    public string ApiServerBaseUrl => $"{Scheme}://{Host}:{ApiServerPort}";
    public string ApiClientBaseUrl => $"{Scheme}://{Host}:{ApiClientPort}";
    public string OutputBaseUrl => $"{Scheme}://{Host}:{OutputPort}";

    public string WorkerDataEditAddress => $"{Scheme}://{Host}:{WorkerDataEditPort}";
    public string WorkerDbAccessAddress => $"{Scheme}://{Host}:{WorkerDbAccessPort}";
    public string WorkerOutputAddress => $"{Scheme}://{Host}:{WorkerOutputPort}";
    public string WorkerAlignmentAddress => $"{Scheme}://{Host}:{WorkerAlignmentPort}";
}
