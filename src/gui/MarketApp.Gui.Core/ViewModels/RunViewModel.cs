using System.Collections.ObjectModel;
using System.Text.Json;

namespace MarketApp.Gui.Core;

public class RunViewModel : ViewModelBase
{
    private const int MaxProgressEvents = 500;

    private readonly IEngineBridgeService _engineBridge;
    private readonly IUserSettingsService _settings;
    private CancellationTokenSource? _cts;
    private string _configPath = "config/config.yaml";
    private string _outputDirectory = "outputs/runs/local";
    private string _pythonPath = "python";
    private string _marketDataSourceDirectory;
    private string _marketDataDestinationDirectory = "market_app/data/ohlcv_raw";
    private string _corpusDataSourceDirectory;
    private string _corpusDataDestinationDirectory = "market_app/data/exogenous/raw";
    private string _corpusNormalizedDirectory = "market_app/data/gdelt";
    private string _preprocessorManifestPath;
    private string _marketRegistryPath;
    private string _gdeltRegistryPath;
    private string _gdeltJoinReadyCsvPath;
    private bool _usePreprocessedSnapshot = true;
    private string _preprocessedStatus = "Preprocessed snapshot not applied yet.";
    private double _progress;
    private string _status = "Idle";
    private bool _isRunning;
    private bool _runBuildLinkedAfterRun = true;
    private bool _runEvaluateAfterRun = true;

    public RunViewModel(IEngineBridgeService engineBridge, IUserSettingsService settings)
    {
        _engineBridge = engineBridge;
        _settings = settings;
        _pythonPath = _settings.GetPythonPath() ?? "python";

        var desktop = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);
        if (string.IsNullOrWhiteSpace(desktop))
        {
            desktop = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Desktop");
        }

        _marketDataSourceDirectory = Path.Combine(desktop, "Market_Files");
        _corpusDataSourceDirectory = Path.Combine(desktop, "NLP Corpus");

        var workingCsvDir = Path.Combine(desktop, "Working CSV Files");
        _preprocessorManifestPath = ResolveLatestManifestPath(workingCsvDir);
        _marketRegistryPath = Path.Combine(workingCsvDir, "_preprocessor_state", "market_processed_registry.json");
        _gdeltRegistryPath = Path.Combine(workingCsvDir, "_preprocessor_state", "gdelt_processed_registry.json");
        _gdeltJoinReadyCsvPath = Path.Combine(workingCsvDir, "_preprocessor_state", "gdelt_daily_join_ready.csv");

        Title = "Run Orchestration";
        StartCommand = new AsyncRelayCommand(StartRunAsync, () => !IsRunning);
        ValidateConfigCommand = new AsyncRelayCommand(ValidateConfigAsync, () => !IsRunning);
        IngestMarketDataCommand = new AsyncRelayCommand(IngestMarketDataAsync, () => !IsRunning);
        IngestCorpusDataCommand = new AsyncRelayCommand(IngestCorpusDataAsync, () => !IsRunning);
        ProcessLatestDataCommand = new AsyncRelayCommand(ProcessLatestDataAsync, () => !IsRunning);
        ApplyPreprocessedSnapshotCommand = new AsyncRelayCommand(ApplyPreprocessedSnapshotAsync, () => !IsRunning);
        CancelCommand = new RelayCommand(CancelRun, () => IsRunning);
    }

    public ObservableCollection<ProgressEvent> ProgressEvents { get; } = new();

    public string ConfigPath
    {
        get => _configPath;
        set => SetProperty(ref _configPath, value);
    }

    public string OutputDirectory
    {
        get => _outputDirectory;
        set => SetProperty(ref _outputDirectory, value);
    }

    public string PythonPath
    {
        get => _pythonPath;
        set
        {
            if (SetProperty(ref _pythonPath, value))
            {
                _settings.SetPythonPath(value);
            }
        }
    }

    public string MarketDataSourceDirectory
    {
        get => _marketDataSourceDirectory;
        set => SetProperty(ref _marketDataSourceDirectory, value);
    }

    public string MarketDataDestinationDirectory
    {
        get => _marketDataDestinationDirectory;
        set => SetProperty(ref _marketDataDestinationDirectory, value);
    }

    public string CorpusDataSourceDirectory
    {
        get => _corpusDataSourceDirectory;
        set => SetProperty(ref _corpusDataSourceDirectory, value);
    }

    public string CorpusDataDestinationDirectory
    {
        get => _corpusDataDestinationDirectory;
        set => SetProperty(ref _corpusDataDestinationDirectory, value);
    }

    public string CorpusNormalizedDirectory
    {
        get => _corpusNormalizedDirectory;
        set => SetProperty(ref _corpusNormalizedDirectory, value);
    }

    public string PreprocessorManifestPath
    {
        get => _preprocessorManifestPath;
        set => SetProperty(ref _preprocessorManifestPath, value);
    }

    public string MarketRegistryPath
    {
        get => _marketRegistryPath;
        set => SetProperty(ref _marketRegistryPath, value);
    }

    public string GdeltRegistryPath
    {
        get => _gdeltRegistryPath;
        set => SetProperty(ref _gdeltRegistryPath, value);
    }

    public string GdeltJoinReadyCsvPath
    {
        get => _gdeltJoinReadyCsvPath;
        set => SetProperty(ref _gdeltJoinReadyCsvPath, value);
    }

    public bool UsePreprocessedSnapshot
    {
        get => _usePreprocessedSnapshot;
        set => SetProperty(ref _usePreprocessedSnapshot, value);
    }

    public string PreprocessedStatus
    {
        get => _preprocessedStatus;
        set => SetProperty(ref _preprocessedStatus, value);
    }

    public bool RunBuildLinkedAfterRun
    {
        get => _runBuildLinkedAfterRun;
        set => SetProperty(ref _runBuildLinkedAfterRun, value);
    }

    public bool RunEvaluateAfterRun
    {
        get => _runEvaluateAfterRun;
        set => SetProperty(ref _runEvaluateAfterRun, value);
    }

    public double Progress
    {
        get => _progress;
        set => SetProperty(ref _progress, value);
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public bool IsRunning
    {
        get => _isRunning;
        set
        {
            if (SetProperty(ref _isRunning, value))
            {
                StartCommand.RaiseCanExecuteChanged();
                ValidateConfigCommand.RaiseCanExecuteChanged();
                IngestMarketDataCommand.RaiseCanExecuteChanged();
                IngestCorpusDataCommand.RaiseCanExecuteChanged();
                ProcessLatestDataCommand.RaiseCanExecuteChanged();
                ApplyPreprocessedSnapshotCommand.RaiseCanExecuteChanged();
                CancelCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public AsyncRelayCommand StartCommand { get; }
    public AsyncRelayCommand ValidateConfigCommand { get; }
    public AsyncRelayCommand IngestMarketDataCommand { get; }
    public AsyncRelayCommand IngestCorpusDataCommand { get; }
    public AsyncRelayCommand ProcessLatestDataCommand { get; }
    public AsyncRelayCommand ApplyPreprocessedSnapshotCommand { get; }
    public RelayCommand CancelCommand { get; }

    private async Task StartRunAsync()
    {
        if (UsePreprocessedSnapshot)
        {
            ApplyPreprocessedSnapshotEnvironment(emitProgressEvents: true);
        }

        _cts = new CancellationTokenSource();
        ProgressEvents.Clear();
        Progress = 0;
        Status = "Starting engine run";
        IsRunning = true;

        var sawError = false;
        try
        {
            var request = new EngineRunRequest(
                ConfigPath: ConfigPath,
                OutDirectory: OutputDirectory,
                Offline: true,
                PythonPath: string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath,
                RunBuildLinked: RunBuildLinkedAfterRun,
                RunEvaluate: RunEvaluateAfterRun,
                IncludeRawGdelt: false,
                WatchlistPath: null);

            await foreach (var evt in _engineBridge.RunAsync(request, _cts.Token))
            {
                if (evt.Pct.HasValue)
                {
                    Progress = Math.Clamp(evt.Pct.Value, 0, 1);
                }

                Status = evt.Message;
                if (string.Equals(evt.Type, "error", StringComparison.OrdinalIgnoreCase))
                {
                    sawError = true;
                }

                AppendProgressEvent(evt);
            }

            if (!sawError)
            {
                Status = "Completed";
                Progress = 1;
            }
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
            AppendProgressEvent(new ProgressEvent(
                Type: "error",
                Stage: "run",
                Message: "Run canceled",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("INTERRUPTED", "Canceled by user", null)));
        }
        finally
        {
            IsRunning = false;
        }
    }

    private async Task ValidateConfigAsync()
    {
        if (UsePreprocessedSnapshot)
        {
            ApplyPreprocessedSnapshotEnvironment(emitProgressEvents: true);
        }

        Status = "Validating config";
        var validation = await _engineBridge.ValidateConfigAsync(
            ConfigPath,
            string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath).ConfigureAwait(false);

        if (validation.Valid)
        {
            Status = "Config is valid";
            AppendProgressEvent(new ProgressEvent(
                Type: "stage_end",
                Stage: "validate_config",
                Message: "Config valid",
                Pct: null,
                Timestamp: DateTime.UtcNow));
            return;
        }

        Status = $"Config invalid ({validation.Errors.Count} issue(s))";
        foreach (var issue in validation.Errors)
        {
            AppendProgressEvent(new ProgressEvent(
                Type: issue.Severity.Equals("warning", StringComparison.OrdinalIgnoreCase) ? "warning" : "error",
                Stage: "validate_config",
                Message: $"[{issue.Path}] {issue.Message}",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: issue.Severity.Equals("warning", StringComparison.OrdinalIgnoreCase)
                    ? null
                    : new ProgressError("INVALID_CONFIG", issue.Message, null)));
        }
    }

    private async Task IngestMarketDataAsync()
    {
        if (string.IsNullOrWhiteSpace(MarketDataSourceDirectory) || !Directory.Exists(Path.GetFullPath(MarketDataSourceDirectory)))
        {
            Status = "Market source folder not found";
            AppendProgressEvent(new ProgressEvent(
                Type: "error",
                Stage: "ingest_market",
                Message: $"Source folder missing: {MarketDataSourceDirectory}",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("MISSING_INPUT", "Market source folder was not found", null)));
            return;
        }

        if (string.IsNullOrWhiteSpace(MarketDataDestinationDirectory))
        {
            Status = "Market destination folder is required";
            return;
        }

        await ExecuteSingleCommandAsync(
            stage: "ingest_market",
            startMessage: "Ingesting market data",
            successMessage: "Market data ingestion complete",
            command: token => _engineBridge.ImportOhlcvAsync(
                sourceDirectory: MarketDataSourceDirectory,
                destinationDirectory: MarketDataDestinationDirectory,
                pythonPath: string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath,
                normalize: true,
                cancellationToken: token)).ConfigureAwait(false);
    }

    private async Task IngestCorpusDataAsync()
    {
        if (string.IsNullOrWhiteSpace(CorpusDataSourceDirectory) || !Directory.Exists(Path.GetFullPath(CorpusDataSourceDirectory)))
        {
            Status = "Corpus source folder not found";
            AppendProgressEvent(new ProgressEvent(
                Type: "error",
                Stage: "ingest_corpus",
                Message: $"Source folder missing: {CorpusDataSourceDirectory}",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("MISSING_INPUT", "Corpus source folder was not found", null)));
            return;
        }

        if (string.IsNullOrWhiteSpace(CorpusDataDestinationDirectory))
        {
            Status = "Corpus destination folder is required";
            return;
        }

        await ExecuteSingleCommandAsync(
            stage: "ingest_corpus",
            startMessage: "Ingesting corpus data",
            successMessage: "Corpus ingestion complete",
            command: token => _engineBridge.ImportExogenousAsync(
                sourceDirectory: CorpusDataSourceDirectory,
                destinationDirectory: CorpusDataDestinationDirectory,
                pythonPath: string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath,
                normalize: true,
                normalizedDestinationDirectory: CorpusNormalizedDirectory,
                cancellationToken: token)).ConfigureAwait(false);
    }

    private async Task ProcessLatestDataAsync()
    {
        if (UsePreprocessedSnapshot)
        {
            ApplyPreprocessedSnapshotEnvironment(emitProgressEvents: true);
        }

        if (!RunBuildLinkedAfterRun && !RunEvaluateAfterRun)
        {
            Status = "Enable at least one processing checkbox";
            AppendProgressEvent(new ProgressEvent(
                Type: "warning",
                Stage: "process_latest",
                Message: "Nothing selected. Enable 'Run linked corpus' and/or 'Run evaluate'.",
                Pct: null,
                Timestamp: DateTime.UtcNow));
            return;
        }

        _cts = new CancellationTokenSource();
        IsRunning = true;
        Progress = 0;
        Status = "Processing latest ingested data";

        var python = string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath;
        var runDir = Path.GetFullPath(OutputDirectory);
        Directory.CreateDirectory(runDir);

        try
        {
            if (RunBuildLinkedAfterRun)
            {
                AppendProgressEvent(new ProgressEvent(
                    Type: "stage_start",
                    Stage: "corpus_build_linked",
                    Message: "Building linked market + corpus features",
                    Pct: 0.1,
                    Timestamp: DateTime.UtcNow));

                var linkedResult = await _engineBridge.BuildLinkedAsync(
                    configPath: ConfigPath,
                    outDirectory: Path.Combine(runDir, "corpus_linked"),
                    pythonPath: python,
                    includeRawGdelt: false,
                    cancellationToken: _cts.Token).ConfigureAwait(false);

                if (!HandleCommandResult("corpus_build_linked", linkedResult, "Linked corpus build complete", 0.55))
                {
                    return;
                }
            }

            if (RunEvaluateAfterRun)
            {
                AppendProgressEvent(new ProgressEvent(
                    Type: "stage_start",
                    Stage: "evaluate",
                    Message: "Running evaluation artifacts",
                    Pct: 0.65,
                    Timestamp: DateTime.UtcNow));

                var evalResult = await _engineBridge.EvaluateAsync(
                    configPath: ConfigPath,
                    outDirectory: runDir,
                    pythonPath: python,
                    cancellationToken: _cts.Token).ConfigureAwait(false);

                if (!HandleCommandResult("evaluate", evalResult, "Evaluation complete", 1.0))
                {
                    return;
                }
            }

            Status = "Processing complete";
            Progress = 1.0;
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
            AppendProgressEvent(new ProgressEvent(
                Type: "error",
                Stage: "process_latest",
                Message: "Processing canceled",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("INTERRUPTED", "Canceled by user", null)));
        }
        finally
        {
            IsRunning = false;
        }
    }

    private Task ApplyPreprocessedSnapshotAsync()
    {
        ApplyPreprocessedSnapshotEnvironment(emitProgressEvents: true);
        return Task.CompletedTask;
    }

    private void ApplyPreprocessedSnapshotEnvironment(bool emitProgressEvents)
    {
        if (!UsePreprocessedSnapshot)
        {
            PreprocessedStatus = "Preprocessed snapshot disabled.";
            return;
        }

        var snapshot = ResolveSnapshot();

        if (!string.IsNullOrWhiteSpace(snapshot.ManifestPath))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_PREPROCESSOR_MANIFEST_PATH", snapshot.ManifestPath);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.MarketRegistryPath))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_MARKET_REGISTRY_PATH", snapshot.MarketRegistryPath);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.GdeltRegistryPath))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_GDELT_REGISTRY_PATH", snapshot.GdeltRegistryPath);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.GdeltJoinReadyCsvPath))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_GDELT_JOIN_READY_CSV", snapshot.GdeltJoinReadyCsvPath);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.OhlcvDirectory) && Directory.Exists(snapshot.OhlcvDirectory))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_OHLCV_DIR", snapshot.OhlcvDirectory);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.ExogenousDirectory) && Directory.Exists(snapshot.ExogenousDirectory))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_EXOGENOUS_DAILY_DIR", snapshot.ExogenousDirectory);
            Environment.SetEnvironmentVariable("MARKET_APP_GDELT_DIR", snapshot.ExogenousDirectory);
        }

        if (!string.IsNullOrWhiteSpace(snapshot.GdeltRawDirectory) && Directory.Exists(snapshot.GdeltRawDirectory))
        {
            Environment.SetEnvironmentVariable("MARKET_APP_GDELT_RAW_DIR", snapshot.GdeltRawDirectory);
        }

        var messages = new List<string>();
        if (string.IsNullOrWhiteSpace(snapshot.OhlcvDirectory) || !Directory.Exists(snapshot.OhlcvDirectory))
        {
            messages.Add("OHLCV directory not found");
        }
        if (string.IsNullOrWhiteSpace(snapshot.GdeltJoinReadyCsvPath) || !File.Exists(snapshot.GdeltJoinReadyCsvPath))
        {
            messages.Add("GDELT join-ready CSV not found");
        }

        if (messages.Count == 0)
        {
            PreprocessedStatus = "Preprocessed snapshot applied (OHLCV + GDELT overrides active).";
            if (emitProgressEvents)
            {
                AppendProgressEvent(new ProgressEvent(
                    Type: "stage_end",
                    Stage: "preprocessed_snapshot",
                    Message: "Applied preprocessed snapshot paths",
                    Pct: null,
                    Timestamp: DateTime.UtcNow));
            }
        }
        else
        {
            PreprocessedStatus = "Preprocessed snapshot applied with warnings: " + string.Join("; ", messages) + ".";
            if (emitProgressEvents)
            {
                AppendProgressEvent(new ProgressEvent(
                    Type: "warning",
                    Stage: "preprocessed_snapshot",
                    Message: PreprocessedStatus,
                    Pct: null,
                    Timestamp: DateTime.UtcNow));
            }
        }
    }

    private PreprocessedSnapshot ResolveSnapshot()
    {
        var snapshot = new PreprocessedSnapshot
        {
            ManifestPath = NormalizePath(PreprocessorManifestPath),
            MarketRegistryPath = NormalizePath(MarketRegistryPath),
            GdeltRegistryPath = NormalizePath(GdeltRegistryPath),
            GdeltJoinReadyCsvPath = NormalizePath(GdeltJoinReadyCsvPath),
        };

        if (!string.IsNullOrWhiteSpace(snapshot.ManifestPath) && File.Exists(snapshot.ManifestPath))
        {
            try
            {
                using var stream = File.OpenRead(snapshot.ManifestPath);
                using var doc = JsonDocument.Parse(stream);
                var root = doc.RootElement;

                if (TryGetString(root, "working_csv_files_dir", out var workingCsvDir) && !string.IsNullOrWhiteSpace(workingCsvDir))
                {
                    snapshot.OhlcvDirectory = workingCsvDir;
                }

                if (TryGetString(root, "market_registry_path", out var marketRegistry) && !string.IsNullOrWhiteSpace(marketRegistry))
                {
                    snapshot.MarketRegistryPath = marketRegistry;
                }

                if (TryGetString(root, "gdelt_corpus_dir", out var gdeltRawDir) && !string.IsNullOrWhiteSpace(gdeltRawDir))
                {
                    snapshot.GdeltRawDirectory = gdeltRawDir;
                }

                if (root.TryGetProperty("gdelt_cache", out var gdeltCache) && gdeltCache.ValueKind == JsonValueKind.Object)
                {
                    if (TryGetString(gdeltCache, "registry_path", out var gdeltRegistry) && !string.IsNullOrWhiteSpace(gdeltRegistry))
                    {
                        snapshot.GdeltRegistryPath = gdeltRegistry;
                    }

                    if (TryGetString(gdeltCache, "daily_join_ready_csv", out var joinReadyCsv) && !string.IsNullOrWhiteSpace(joinReadyCsv))
                    {
                        snapshot.GdeltJoinReadyCsvPath = joinReadyCsv;
                    }
                }
            }
            catch (Exception ex)
            {
                AppendProgressEvent(new ProgressEvent(
                    Type: "warning",
                    Stage: "preprocessed_snapshot",
                    Message: $"Failed to parse manifest: {ex.Message}",
                    Pct: null,
                    Timestamp: DateTime.UtcNow));
            }
        }

        if (string.IsNullOrWhiteSpace(snapshot.OhlcvDirectory) && !string.IsNullOrWhiteSpace(snapshot.MarketRegistryPath))
        {
            var inferred = Path.GetDirectoryName(snapshot.MarketRegistryPath);
            if (!string.IsNullOrWhiteSpace(inferred))
            {
                var stateDir = new DirectoryInfo(inferred);
                if (stateDir.Exists && stateDir.Parent is not null)
                {
                    snapshot.OhlcvDirectory = stateDir.Parent.FullName;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(snapshot.GdeltJoinReadyCsvPath))
        {
            snapshot.ExogenousDirectory = Path.GetDirectoryName(snapshot.GdeltJoinReadyCsvPath);
        }

        PreprocessorManifestPath = snapshot.ManifestPath ?? PreprocessorManifestPath;
        MarketRegistryPath = snapshot.MarketRegistryPath ?? MarketRegistryPath;
        GdeltRegistryPath = snapshot.GdeltRegistryPath ?? GdeltRegistryPath;
        GdeltJoinReadyCsvPath = snapshot.GdeltJoinReadyCsvPath ?? GdeltJoinReadyCsvPath;

        return snapshot;
    }

    private async Task ExecuteSingleCommandAsync(
        string stage,
        string startMessage,
        string successMessage,
        Func<CancellationToken, Task<EngineCommandResult>> command)
    {
        _cts = new CancellationTokenSource();
        IsRunning = true;
        Progress = 0;
        Status = startMessage;

        AppendProgressEvent(new ProgressEvent(
            Type: "stage_start",
            Stage: stage,
            Message: startMessage,
            Pct: 0,
            Timestamp: DateTime.UtcNow));

        try
        {
            var result = await command(_cts.Token).ConfigureAwait(false);
            HandleCommandResult(stage, result, successMessage, 1.0);
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
            AppendProgressEvent(new ProgressEvent(
                Type: "error",
                Stage: stage,
                Message: "Operation canceled",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("INTERRUPTED", "Canceled by user", null)));
        }
        finally
        {
            IsRunning = false;
        }
    }

    private bool HandleCommandResult(string stage, EngineCommandResult result, string successMessage, double successProgress)
    {
        AppendCommandOutputEvents(stage, result);

        if (result.ExitCode == 0)
        {
            Status = successMessage;
            Progress = successProgress;
            AppendProgressEvent(new ProgressEvent(
                Type: "stage_end",
                Stage: stage,
                Message: successMessage,
                Pct: successProgress,
                Timestamp: DateTime.UtcNow));
            return true;
        }

        var detail = string.IsNullOrWhiteSpace(result.Stderr)
            ? $"Command failed with exit code {result.ExitCode}."
            : result.Stderr.Trim();

        Status = $"{stage} failed";
        AppendProgressEvent(new ProgressEvent(
            Type: "error",
            Stage: stage,
            Message: $"{stage} failed (exit {result.ExitCode})",
            Pct: null,
            Timestamp: DateTime.UtcNow,
            Error: new ProgressError("RUNTIME_FAILURE", detail, null)));
        return false;
    }

    private void AppendCommandOutputEvents(string stage, EngineCommandResult result)
    {
        var stdoutLines = SplitLines(result.Stdout).Take(8);
        foreach (var line in stdoutLines)
        {
            AppendProgressEvent(new ProgressEvent(
                Type: "warning",
                Stage: stage,
                Message: line,
                Pct: null,
                Timestamp: DateTime.UtcNow,
                RawLine: line));
        }

        var stderrLines = SplitLines(result.Stderr).Take(8);
        foreach (var line in stderrLines)
        {
            AppendProgressEvent(new ProgressEvent(
                Type: "warning",
                Stage: stage,
                Message: line,
                Pct: null,
                Timestamp: DateTime.UtcNow,
                RawLine: line));
        }
    }

    private static IEnumerable<string> SplitLines(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return Array.Empty<string>();
        }

        return text
            .Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim())
            .Where(line => !string.IsNullOrWhiteSpace(line));
    }

    private void AppendProgressEvent(ProgressEvent evt)
    {
        ProgressEvents.Add(evt);
        if (ProgressEvents.Count > MaxProgressEvents)
        {
            ProgressEvents.RemoveAt(0);
        }
    }

    private void CancelRun()
    {
        _cts?.Cancel();
    }

    private static bool TryGetString(JsonElement element, string name, out string? value)
    {
        value = null;
        if (!element.TryGetProperty(name, out var node) || node.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        value = node.GetString();
        return !string.IsNullOrWhiteSpace(value);
    }

    private static string? NormalizePath(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        try
        {
            return Path.GetFullPath(value.Trim());
        }
        catch
        {
            return value.Trim();
        }
    }

    private static string ResolveLatestManifestPath(string workingCsvDir)
    {
        if (Directory.Exists(workingCsvDir))
        {
            var latest = Directory
                .GetFiles(workingCsvDir, "daily_market_preprocessor_manifest_*.json")
                .OrderByDescending(path => path, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(latest))
            {
                return latest;
            }
        }

        return Path.Combine(workingCsvDir, "daily_market_preprocessor_manifest_latest.json");
    }

    private sealed class PreprocessedSnapshot
    {
        public string? ManifestPath { get; set; }
        public string? MarketRegistryPath { get; set; }
        public string? GdeltRegistryPath { get; set; }
        public string? GdeltJoinReadyCsvPath { get; set; }
        public string? OhlcvDirectory { get; set; }
        public string? ExogenousDirectory { get; set; }
        public string? GdeltRawDirectory { get; set; }
    }
}
