namespace MarketApp.Gui.Core;

public sealed class RunCompareService : IRunCompareService
{
    private readonly IEngineBridgeService _engineBridge;

    public RunCompareService(IEngineBridgeService engineBridge)
    {
        _engineBridge = engineBridge;
    }

    public async Task<RunDiffResult?> CompareAsync(
        RunSummary? runA,
        RunSummary? runB,
        string? pythonPath,
        CancellationToken cancellationToken = default)
    {
        if (runA is null || runB is null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(runA.RunDirectory) || string.IsNullOrWhiteSpace(runB.RunDirectory))
        {
            return null;
        }

        if (string.Equals(runA.RunDirectory, runB.RunDirectory, StringComparison.OrdinalIgnoreCase))
        {
            return new RunDiffResult(
                runA.RunId,
                runB.RunId,
                new RunDiffSummary(0, 0, 0, 0),
                Array.Empty<RunDiffRow>());
        }

        return await _engineBridge.DiffRunsAsync(
            runA.RunDirectory,
            runB.RunDirectory,
            pythonPath,
            cancellationToken).ConfigureAwait(false);
    }
}
