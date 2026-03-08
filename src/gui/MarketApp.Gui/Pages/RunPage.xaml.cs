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
            "preprocessed_snapshot" => (
                "Preprocessed Snapshot",
                "This mode tells the app to use your already-cleaned market and GDELT files instead of guessing paths."),
            "manifest_json" => (
                "Manifest JSON",
                "This file is a summary report from your preprocessor. The app reads it to find the right data folders."),
            "market_registry_json" => (
                "Market Registry JSON",
                "This file tracks which market files were already processed, so work is repeatable and traceable."),
            "gdelt_registry_json" => (
                "GDELT Registry JSON",
                "This file tracks which GDELT corpus files were already parsed by your preprocessor."),
            "gdelt_join_ready_csv" => (
                "GDELT Join-Ready CSV",
                "This is the daily GDELT table ready to join with market dates. It is the main corpus file used in scoring."),
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
