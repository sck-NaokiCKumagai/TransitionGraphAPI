namespace TransitionGraphDtos.Wpf;

/// <summary>
/// WPFとのI/Fで使用するDTO
/// </summary>
public sealed class WpfQueryDto
{
    public string? requestId { get; set; }
    public List<string>? parameters { get; set; }
    public string? guid { get; set; }
}
