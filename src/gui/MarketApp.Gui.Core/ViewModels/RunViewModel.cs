using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class RunViewModel : ViewModelBase
{
    private readonly SimulatedRunOrchestrator _orchestrator;
    private CancellationTokenSource? _cts;
    private string _configPath = "config/config.yaml";
    private string _outputDirectory = "outputs/runs/local";
    private double _progress;
    private string _status = "Idle";
    private bool _isRunning;

    public RunViewModel(SimulatedRunOrchestrator orchestrator)
    {
        _orchestrator = orchestrator;
        Title = "Run Orchestration";
        StartCommand = new AsyncRelayCommand(StartRunAsync, () => !IsRunning);
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
                CancelCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public AsyncRelayCommand StartCommand { get; }
    public RelayCommand CancelCommand { get; }

    private async Task StartRunAsync()
    {
        _cts = new CancellationTokenSource();
        ProgressEvents.Clear();
        Progress = 0;
        Status = "Starting";
        IsRunning = true;

        try
        {
            await foreach (var evt in _orchestrator.RunAsync(_cts.Token))
            {
                Progress = evt.Pct ?? Progress;
                Status = evt.Message;
                ProgressEvents.Add(evt);
            }

            Status = "Completed";
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
        }
        finally
        {
            Progress = Math.Min(1, Progress);
            IsRunning = false;
        }
    }

    private void CancelRun()
    {
        _cts?.Cancel();
    }
}
