using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class UniversePage : ContentPage
{
    public UniversePage(UniverseViewModel viewModel)
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
            "universe_symbols" => (
                "Universe and Scored Symbols",
                "This table is the full list of symbols the run scored. You can filter it and pick one symbol to inspect."),
            "symbol_detail" => (
                "Symbol Detail",
                "This panel explains one chosen symbol, including charts, explain notes, and quality checks."),
            _ => (
                "Info",
                "This section helps you inspect individual symbols after they were scored.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
