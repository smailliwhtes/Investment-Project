namespace MarketApp.Gui.Core.Models;

public sealed record EngineRunRequest(string PythonPath, string ConfigPath, string OutDir, bool Offline, bool ProgressJsonl);

public sealed record EngineProgressEvent(DateTime Timestamp, string Stage, int Percent, string Message, string? ArtifactPath = null);

public sealed record EngineRunResult(int ExitCode, string StdoutLogPath, string StderrLogPath, string RunDir);

public sealed record ConfigValidationError(string Path, string Message);

public sealed record ConfigValidationResult(bool IsValid, IReadOnlyList<ConfigValidationError> Errors);
