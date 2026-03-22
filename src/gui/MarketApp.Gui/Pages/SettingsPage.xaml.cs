using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class SettingsPage : ContentPage
{
    private readonly SettingsViewModel _viewModel;

    public SettingsPage(SettingsViewModel viewModel)
    {
        InitializeComponent();
        _viewModel = viewModel;
        BindingContext = _viewModel;
        Loaded += OnLoaded;
    }

    private async void OnLoaded(object? sender, EventArgs e)
    {
        await _viewModel.InitializeAsync();
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
            "engine_settings" => (
                "Engine Settings",
                "These values tell the app which config file and Python runner to use."),
            "secrets" => (
                "Optional Live-Data Keys",
                "These API keys are optional. Leave them blank if you only want offline practice. If you add them, the app stores them securely on your computer."),
            _ => (
                "Info",
                "This setting controls how the app connects to local engine and secure keys.")
        };

        await DisplayAlert(title, message, "Got it");
    }
}
