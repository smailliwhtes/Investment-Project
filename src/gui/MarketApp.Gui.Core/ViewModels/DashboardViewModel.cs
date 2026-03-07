namespace MarketApp.Gui.Core;

public class DashboardViewModel : ViewModelBase
{
    private DashboardSummary? _summary;

    public DashboardViewModel(SampleDataService dataService)
    {
        Title = "Dashboard";
        _summary = dataService.GetDashboard();
    }

    public DashboardSummary? Summary
    {
        get => _summary;
        set
        {
            if (SetProperty(ref _summary, value))
            {
                OnPropertyChanged(nameof(LastRun));
                OnPropertyChanged(nameof(TopSymbols));
                OnPropertyChanged(nameof(RecentLogs));
                OnPropertyChanged(nameof(CauseEffect));
                OnPropertyChanged(nameof(BacktestMetrics));
                OnPropertyChanged(nameof(QualitySnapshot));
            }
        }
    }

    public RunSummary? LastRun => _summary?.LastRun;
    public IReadOnlyList<ScoreRow> TopSymbols => _summary?.TopSymbols ?? Array.Empty<ScoreRow>();
    public IReadOnlyList<LogEntry> RecentLogs => _summary?.RecentLogs ?? Array.Empty<LogEntry>();
    public CauseEffectSnapshot? CauseEffect => _summary?.CauseEffect;
    public IReadOnlyList<BacktestMetricRow> BacktestMetrics => _summary?.BacktestMetrics ?? Array.Empty<BacktestMetricRow>();
    public RunQualitySnapshot? QualitySnapshot => _summary?.QualitySnapshot;
}
