using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class RunsViewModel : ViewModelBase
{
    private RunSummary? _selectedRun;

    public RunsViewModel(SampleDataService dataService)
    {
        Title = "Runs History";
        Runs = new ObservableCollection<RunSummary>(dataService.GetRunHistory());
        SelectedRun = Runs.FirstOrDefault();
    }

    public ObservableCollection<RunSummary> Runs { get; }

    public RunSummary? SelectedRun
    {
        get => _selectedRun;
        set => SetProperty(ref _selectedRun, value);
    }
}
