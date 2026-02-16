namespace MarketApp.Gui.Core;

public class LogsViewModel : ViewModelBase
{
    private IReadOnlyList<LogEntry> _entries;

    public LogsViewModel(SampleDataService dataService)
    {
        Title = "Logs";
        _entries = dataService.GetLogs();
    }

    public IReadOnlyList<LogEntry> Entries
    {
        get => _entries;
        set
        {
            if (SetProperty(ref _entries, value))
            {
                OnPropertyChanged(nameof(CombinedLog));
            }
        }
    }

    public string CombinedLog => string.Join(
        Environment.NewLine,
        _entries.Select(e => $"[{e.Timestamp:HH:mm:ss}] {e.Level.ToUpperInvariant()} - {e.Message}"));
}
