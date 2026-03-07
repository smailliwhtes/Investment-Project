using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class RunViewModel : ViewModelBase
{
    private readonly IEngineBridgeService _engineBridge;
    private readonly IUserSettingsService _settings;
    private CancellationTokenSource? _cts;
    private string _configPath = "config/config.yaml";
    private string _outputDirectory = "outputs/runs/local";
    private string _pythonPath = "python";
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

        Title = "Run Orchestration";
        StartCommand = new AsyncRelayCommand(StartRunAsync, () => !IsRunning);
        ValidateConfigCommand = new AsyncRelayCommand(ValidateConfigAsync, () => !IsRunning);
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
                CancelCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public AsyncRelayCommand StartCommand { get; }
    public AsyncRelayCommand ValidateConfigCommand { get; }
    public RelayCommand CancelCommand { get; }

    private async Task StartRunAsync()
    {
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

                ProgressEvents.Add(evt);
                if (ProgressEvents.Count > 500)
                {
                    ProgressEvents.RemoveAt(0);
                }
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
            ProgressEvents.Add(new ProgressEvent(
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
        Status = "Validating config";
        var validation = await _engineBridge.ValidateConfigAsync(
            ConfigPath,
            string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath).ConfigureAwait(false);

        if (validation.Valid)
        {
            Status = "Config is valid";
            ProgressEvents.Add(new ProgressEvent(
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
            ProgressEvents.Add(new ProgressEvent(
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

    private void CancelRun()
    {
        _cts?.Cancel();
    }
}
