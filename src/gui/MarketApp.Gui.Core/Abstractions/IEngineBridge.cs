using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Core.Abstractions;

public interface IEngineBridge
{
    Task<EngineRunResult> RunAsync(EngineRunRequest request, IProgress<EngineProgressEvent> progress, CancellationToken cancellationToken);
    Task<ConfigValidationResult> ValidateConfigAsync(string configPath, CancellationToken cancellationToken);
}
