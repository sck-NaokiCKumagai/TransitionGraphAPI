using System.Collections.Generic;

namespace DXC.EngPF.Core.Grid;

/// <summary>
/// RequestId=10 parameter DTO.
///
/// This mirrors the minimal surface used by the real DXC.EngPF.Core.Grid.dll.
/// In this repository (dummy DLL), the values are accepted but not executed.
/// </summary>
public sealed class FilterAndSortParam
{
    public string SecondCacheId { get; set; } = string.Empty;

    public string SortConditionParameter { get; set; } = string.Empty;

    public bool IsManualSort { get; set; }

    public int ManualSortMoveIndex { get; set; }

    public ICollection<int> ManualSortTargetRowIndex { get; set; } = new List<int>();
}
