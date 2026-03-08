using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class RunsPage : ContentPage
{
    public RunsPage(RunsViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }

    private async void OnInfoClicked(object? sender, EventArgs e)
    {
        var key = (sender as Button)?.CommandParameter as string;
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        var (title, message) = key switch
        {
            "run_history" => (
                "Run History",
                "This list shows your past runs. Each row is one full analysis snapshot saved at that time."),
            "run_compare" => (
                "Run Comparison",
                "Pick two runs to see what changed, like score moves, rank moves, and symbols that were added or removed."),
            "run_a" => (
                "Run A",
                "Run A is your baseline run. The app compares Run B against this one."),
            "run_b" => (
                "Run B",
                "Run B is the newer or alternate run you want to compare against Run A."),
            "run_a_quality" => (
                "Run A Quality",
                "This is the model report card for Run A, showing error and direction metrics."),
            _ => (
                "Info",
                "This section helps you review differences between two saved analysis runs.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
