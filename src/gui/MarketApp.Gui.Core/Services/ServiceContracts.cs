namespace MarketApp.Gui.Core;

public sealed record EngineRunRequest(
    string ConfigPath,
    string OutDirectory,
    bool Offline = true,
    string? PythonPath = null,
    bool RunBuildLinked = false,
    bool RunEvaluate = false,
    bool IncludeRawGdelt = false,
    string? WatchlistPath = null
);

public sealed record EngineCommandResult(int ExitCode, string Stdout, string Stderr);

public sealed record ConfigValidationIssue(string Path, string Message, string Severity);

public sealed record ConfigValidationResult(bool Valid, IReadOnlyList<ConfigValidationIssue> Errors, string RawJson);

public interface IEngineBridgeService
{
    IAsyncEnumerable<ProgressEvent> RunAsync(EngineRunRequest request, CancellationToken cancellationToken);

    Task<ConfigValidationResult> ValidateConfigAsync(
        string configPath,
        string? pythonPath,
        CancellationToken cancellationToken = default);

    Task<EngineCommandResult> BuildLinkedAsync(
        string configPath,
        string outDirectory,
        string? pythonPath,
        bool includeRawGdelt,
        CancellationToken cancellationToken = default);

    Task<EngineCommandResult> EvaluateAsync(
        string configPath,
        string outDirectory,
        string? pythonPath,
        CancellationToken cancellationToken = default);

    Task<EngineCommandResult> ImportOhlcvAsync(
        string sourceDirectory,
        string destinationDirectory,
        string? pythonPath,
        bool normalize = true,
        string? dateColumn = null,
        string? delimiter = null,
        CancellationToken cancellationToken = default);

    Task<EngineCommandResult> ImportExogenousAsync(
        string sourceDirectory,
        string destinationDirectory,
        string? pythonPath,
        bool normalize = true,
        string? normalizedDestinationDirectory = null,
        string fileGlob = "*.csv",
        string formatHint = "auto",
        string writeFormat = "csv",
        CancellationToken cancellationToken = default);

    Task<RunDiffResult> DiffRunsAsync(
        string runA,
        string runB,
        string? pythonPath,
        CancellationToken cancellationToken = default);
}

public interface IRunDiscoveryService
{
    Task<IReadOnlyList<RunSummary>> DiscoverRunsAsync(CancellationToken cancellationToken = default);

    string? ResolveOutputsRoot();
}

public interface IRunCompareService
{
    Task<RunDiffResult?> CompareAsync(
        RunSummary? runA,
        RunSummary? runB,
        string? pythonPath,
        CancellationToken cancellationToken = default);
}

public interface IQualityMetricsService
{
    Task<RunQualitySnapshot?> LoadRunQualityAsync(
        RunSummary run,
        CancellationToken cancellationToken = default);
}

public interface IUserSettingsService
{
    string? GetPythonPath();
    void SetPythonPath(string? pythonPath);
}
