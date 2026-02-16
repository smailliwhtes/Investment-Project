using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class UniverseViewModel : ViewModelBase
{
    private string _filterText = string.Empty;
    private ScoreRow? _selectedScore;
    private IReadOnlyList<ScoreRow> _allScores = Array.Empty<ScoreRow>();
    private IReadOnlyList<ScoreRow> _filtered = Array.Empty<ScoreRow>();

    public UniverseViewModel(SampleDataService dataService)
    {
        Title = "Universe & Scores";
        _allScores = dataService.GetScores();
        _filtered = _allScores;
    }

    public IReadOnlyList<ScoreRow> FilteredScores
    {
        get => _filtered;
        private set => SetProperty(ref _filtered, value);
    }

    public string FilterText
    {
        get => _filterText;
        set
        {
            if (SetProperty(ref _filterText, value))
            {
                ApplyFilter();
            }
        }
    }

    public ScoreRow? SelectedScore
    {
        get => _selectedScore;
        set => SetProperty(ref _selectedScore, value);
    }

    private void ApplyFilter()
    {
        if (string.IsNullOrWhiteSpace(_filterText))
        {
            FilteredScores = _allScores;
            return;
        }

        FilteredScores = _allScores
            .Where(s => s.Symbol.Contains(_filterText, StringComparison.OrdinalIgnoreCase) ||
                        s.ThemeLabels.Contains(_filterText, StringComparison.OrdinalIgnoreCase))
            .ToArray();
    }
}
