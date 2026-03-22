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
                "Scored Symbols",
                "This is the saved list of symbols from the run. Search for one name, then open it for a short plain-language read."),
            "symbol_detail" => (
                "Symbol Detail",
                "This panel shows one symbol at a time. Start with the short takeaway, then look at the charts if you want more detail."),
            _ => (
                "Info",
                "This section helps you inspect individual symbols after they were scored.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
