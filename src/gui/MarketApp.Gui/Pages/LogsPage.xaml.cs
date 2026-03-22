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
            "Use this page when something fails. Warnings are caution notes, and errors mean a step stopped.",
            "Got it");
    }
}
