// NuGetインストール
using Google.Protobuf.WellKnownTypes;
// NuGetインストール
using Grpc.Core;
using TransitionGraph.V1;
using Microsoft.Extensions.Hosting;

namespace TransitionGraph.Shared;

/// <summary>
/// HealthService-Healthサービス
/// 処理概略：Healthのサービス処理を行う
/// </summary>
public sealed class HealthService : Health.HealthBase
{
    /// <summary>
    /// Check-チェック処理
    /// 処理概略：チェック処理する
    /// </summary>
    public override Task<HealthReply> Check(Empty request, ServerCallContext context)
        => Task.FromResult(new HealthReply { Status = "OK", Message = "ready" });
}

/// <summary>
/// ControlService-Controlサービス
/// 処理概略：Controlのサービス処理を行う
/// </summary>
public sealed class ControlService : Control.ControlBase
{
    private readonly IHostApplicationLifetime _lifetime;

    /// <summary>
    /// ControlService-Controlサービス
    /// 処理概略：ControlServiceの初期化を行う
    /// </summary>
    public ControlService(IHostApplicationLifetime lifetime) => _lifetime = lifetime;

    /// <summary>
    /// Shutdown-シャットダウン処理
    /// 処理概略：Shutdown処理する
    /// </summary>
    public override Task<ShutdownReply> Shutdown(Empty request, ServerCallContext context)
    {
        _lifetime.StopApplication();
        return Task.FromResult(new ShutdownReply { Message = "shutting down" });
    }
}