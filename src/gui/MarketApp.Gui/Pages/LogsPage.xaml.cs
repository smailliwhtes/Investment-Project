using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class LogsPage : ContentPage
{
    public LogsPage(LogsViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }

    private async void OnInfoClicked(object? sender, EventArgs e)
    {
        await DisplayAlert(
            "Engine and UI Logs",
            "This box shows what the app and engine did step by step, so you can troubleshoot problems quickly.",
            "Got it");
    }
}
