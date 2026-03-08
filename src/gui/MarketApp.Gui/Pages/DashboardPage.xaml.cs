using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class DashboardPage : ContentPage
{
    public DashboardPage(DashboardViewModel viewModel)
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
            "last_run" => (
                "Last Run",
                "This is the most recent full check your app finished. Think of it like the latest report card for the market scan."),
            "universe" => (
                "Universe",
                "This is how many stock symbols the app looked at in this run. Bigger number means more stocks checked."),
            "eligible" => (
                "Eligible",
                "This is how many stocks passed the safety and quality rules. These are the ones allowed into scoring."),
            "cause_effect" => (
                "Cause/Effect Linkage",
                "This section matches market moves with event data. It helps show what news-like events may connect to price changes."),
            "backtest_quality" => (
                "Backtest Quality",
                "This is a report card for prediction models using old data. It shows how close the model guesses were."),
            "metric_model" => (
                "Model",
                "A model is the math recipe used to make predictions. Different recipes can perform better or worse."),
            "metric_mse" => (
                "MSE",
                "MSE is average squared error. Lower is better. Big mistakes are punished extra hard in this score."),
            "metric_mae" => (
                "MAE",
                "MAE is average absolute error. Lower is better. It tells how far off predictions were on average."),
            "metric_accuracy" => (
                "Accuracy",
                "Accuracy is the percent of times the model got direction right in the chosen setup. Higher is better."),
            "metric_f1" => (
                "F1",
                "F1 balances precision and recall into one score. Higher means a better balance of catching true signals while avoiding false alarms."),
            "top_symbols" => (
                "Top Symbols",
                "These are the highest-ranked symbols after scoring. They are sorted from stronger score to weaker score."),
            "top_symbol_symbol" => (
                "Symbol",
                "This is the stock ticker, like AAPL or MSFT."),
            "top_symbol_score" => (
                "Score",
                "This number summarizes how strong the setup looks based on the model rules. Higher score means stronger setup by this system."),
            "top_symbol_lag" => (
                "Lag (d)",
                "Lag tells how old the latest data is, in days. Lower lag is fresher data."),
            "top_symbol_theme" => (
                "Themes",
                "Themes are short labels that describe what category the symbol fit during analysis, like AI or Energy."),
            _ => (
                "Info",
                "This value is part of the market analysis summary for the selected run.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
