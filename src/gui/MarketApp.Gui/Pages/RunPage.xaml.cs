using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class RunPage : ContentPage
{
    public RunPage(RunViewModel viewModel)
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
            "run_orchestration" => (
                "Run Orchestration",
                "This section is where you tell the app what config to use and where to save the run results."),
            "config_path" => (
                "Config Path",
                "This is the recipe file for your run. It tells the engine what data, filters, and rules to use."),
            "output_directory" => (
                "Output Directory",
                "This is the folder where reports and CSV results are saved after each run."),
            "python_path" => (
                "Python Path",
                "This points to the Python app runner. If left as python, the app uses your default Python install."),
            "ingest_process" => (
                "Ingest and Process",
                "Use these buttons to copy in new raw files first, then build processed datasets the models use."),
            "market_source" => (
                "Market Source Folder",
                "This is where your newest raw market files live, like your Desktop market data folder."),
            "market_dest" => (
                "Market App Folder",
                "This is where the app stores imported market files before normalizing them for runs."),
            "corpus_source" => (
                "Corpus Source Folder",
                "This is where your newest event/news corpus files live before importing."),
            "corpus_dest" => (
                "Corpus App Folder",
                "This is where the app stores imported corpus files before creating cleaned features."),
            "corpus_normalized" => (
                "Corpus Normalized",
                "This folder stores cleaned, day-by-day corpus data that the engine can join with market data."),
            "progress_events" => (
                "Progress Events",
                "These lines are the engine's live timeline, so you can see each step and catch errors fast."),
            _ => (
                "Info",
                "This setting controls part of the run workflow for importing, processing, or executing analysis.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
