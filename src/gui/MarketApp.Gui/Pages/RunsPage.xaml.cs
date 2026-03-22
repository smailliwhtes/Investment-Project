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
                "Saved Runs",
                "This list shows your past scans. Each row is one saved snapshot you can reopen or compare."),
            "run_compare" => (
                "Run Comparison",
                "Pick two runs to see the main differences, such as score moves, rank moves, and symbols that were added or removed."),
            "run_a" => (
                "Run A",
                "Run A is the first run in the comparison. Think of it as the starting point."),
            "run_b" => (
                "Run B",
                "Run B is the newer or alternate run you want to compare against Run A."),
            "run_a_quality" => (
                "Run A Model Check",
                "This is a short report card for Run A. Lower miss values are better, and higher hit rate is better."),
            _ => (
                "Info",
                "This section helps you review differences between two saved analysis runs.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
