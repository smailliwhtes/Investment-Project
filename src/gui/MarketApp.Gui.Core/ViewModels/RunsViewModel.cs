using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class RunsViewModel : ViewModelBase
{
    private readonly IRunDiscoveryService _runDiscovery;
    private readonly IRunCompareService _runCompare;
    private readonly IQualityMetricsService _qualityMetrics;
    private readonly IUserSettingsService _settings;

    private RunSummary? _selectedRunA;
    private RunSummary? _selectedRunB;
    private RunDiffResult? _diffResult;
    private RunQualitySnapshot? _selectedRunQuality;
    private string _status = "Ready";

    public RunsViewModel(
        IRunDiscoveryService runDiscovery,
        IRunCompareService runCompare,
        IQualityMetricsService qualityMetrics,
        IUserSettingsService settings)
    {
        _runDiscovery = runDiscovery;
        _runCompare = runCompare;
        _qualityMetrics = qualityMetrics;
        _settings = settings;

        Title = "Saved Runs";
        RefreshCommand = new AsyncRelayCommand(RefreshAsync, () => !IsBusy);
        CompareCommand = new AsyncRelayCommand(CompareRunsAsync, () => !IsBusy && SelectedRunA is not null && SelectedRunB is not null);

        _ = RefreshAsync();
    }

    public ObservableCollection<RunSummary> Runs { get; } = new();

    public RunSummary? SelectedRunA
    {
        get => _selectedRunA;
        set
        {
            if (SetProperty(ref _selectedRunA, value))
            {
                CompareCommand.RaiseCanExecuteChanged();
                _ = LoadSelectedRunQualityAsync();
            }
        }
    }

    public RunSummary? SelectedRunB
    {
        get => _selectedRunB;
        set
        {
            if (SetProperty(ref _selectedRunB, value))
            {
                CompareCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public RunDiffResult? DiffResult
    {
        get => _diffResult;
        private set
        {
            if (SetProperty(ref _diffResult, value))
            {
                OnPropertyChanged(nameof(DiffRows));
                OnPropertyChanged(nameof(DiffSummary));
            }
        }
    }

    public IReadOnlyList<RunDiffRow> DiffRows => DiffResult?.Rows ?? Array.Empty<RunDiffRow>();

    public string DiffSummary => DiffResult is null
        ? "Choose two runs to see what changed."
        : $"Showing {DiffResult.Summary.NSymbols} symbols. {DiffResult.Summary.NNew} are new, {DiffResult.Summary.NRemoved} dropped out, and {DiffResult.Summary.NRankChanged} moved in rank.";

    public RunQualitySnapshot? SelectedRunQuality
    {
        get => _selectedRunQuality;
        private set
        {
            if (SetProperty(ref _selectedRunQuality, value))
            {
                OnPropertyChanged(nameof(SelectedRunMetrics));
            }
        }
    }

    public IReadOnlyList<BacktestMetricRow> SelectedRunMetrics => SelectedRunQuality?.Metrics ?? Array.Empty<BacktestMetricRow>();

    public string Status
    {
        get => _status;
        private set => SetProperty(ref _status, value);
    }

    public AsyncRelayCommand RefreshCommand { get; }
    public AsyncRelayCommand CompareCommand { get; }

    private async Task RefreshAsync()
    {
        IsBusy = true;
        CompareCommand.RaiseCanExecuteChanged();
        try
        {
            var runs = await _runDiscovery.DiscoverRunsAsync().ConfigureAwait(false);
            Runs.Clear();
            foreach (var run in runs)
            {
                Runs.Add(run);
            }

            SelectedRunA = Runs.FirstOrDefault();
            SelectedRunB = Runs.Skip(1).FirstOrDefault() ?? SelectedRunA;
            Status = $"Loaded {Runs.Count} saved run(s)";
        }
        finally
        {
            IsBusy = false;
            CompareCommand.RaiseCanExecuteChanged();
            RefreshCommand.RaiseCanExecuteChanged();
        }
    }

    private async Task LoadSelectedRunQualityAsync()
    {
        if (SelectedRunA is null)
        {
            SelectedRunQuality = null;
            return;
        }

        SelectedRunQuality = await _qualityMetrics.LoadRunQualityAsync(SelectedRunA).ConfigureAwait(false);
    }

    private async Task CompareRunsAsync()
    {
        if (SelectedRunA is null || SelectedRunB is null)
        {
            return;
        }

        IsBusy = true;
        RefreshCommand.RaiseCanExecuteChanged();
        CompareCommand.RaiseCanExecuteChanged();
        Status = "Comparing the selected runs";

        try
        {
            var python = _settings.GetPythonPath();
            DiffResult = await _runCompare.CompareAsync(SelectedRunA, SelectedRunB, python).ConfigureAwait(false);
            Status = DiffResult is null ? "Comparison unavailable" : "Comparison ready";
        }
        catch (Exception ex)
        {
            Status = $"Comparison failed: {ex.Message}";
            DiffResult = null;
        }
        finally
        {
            IsBusy = false;
            RefreshCommand.RaiseCanExecuteChanged();
            CompareCommand.RaiseCanExecuteChanged();
        }
    }
}
