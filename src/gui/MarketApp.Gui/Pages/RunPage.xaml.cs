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
                "Start A Run",
                "Most users only need the top section. Pick the config file, choose where to save the results, and start the run."),
            "config_path" => (
                "Config File",
                "This is the run recipe. It tells the engine which data, filters, and rules to use."),
            "output_directory" => (
                "Save Results To",
                "This is the folder where the app will save the run report and CSV files."),
            "python_path" => (
                "Python Runner",
                "This tells the app which Python to use. If you leave it as python, the app uses your default Python install."),
            "preprocessed_snapshot" => (
                "Advanced Path Setup",
                "You can ignore this unless you already have cleaned files and want the app to use those exact paths."),
            "manifest_json" => (
                "Manifest File",
                "This file tells the app where your cleaned files live."),
            "market_registry_json" => (
                "Price Registry File",
                "This file helps the app reuse your saved price-file setup."),
            "gdelt_registry_json" => (
                "Event Registry File",
                "This file helps the app reuse your saved event-file setup."),
            "gdelt_join_ready_csv" => (
                "Joined Event File",
                "This is the cleaned day-by-day event file that can line up with market dates."),
            "ingest_process" => (
                "Optional Import And Processing",
                "Use these tools only when you want to bring in new price or event files."),
            "market_source" => (
                "Price-Data Source Folder",
                "This is the folder that holds the raw price files you want to import."),
            "market_dest" => (
                "Price-Data App Folder",
                "This is where the app stores imported price files."),
            "corpus_source" => (
                "Event-Data Source Folder",
                "This is the folder that holds the raw event files you want to import."),
            "corpus_dest" => (
                "Event-Data App Folder",
                "This is where the app stores imported event files."),
            "corpus_normalized" => (
                "Cleaned Event Folder",
                "This folder stores the cleaned event data that the app can line up with market data."),
            "progress_events" => (
                "Run Progress",
                "This shows the step-by-step timeline. You only need it if you want to watch the run live or troubleshoot a failure."),
            _ => (
                "Info",
                "This setting controls part of the run workflow for importing, processing, or executing analysis.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
