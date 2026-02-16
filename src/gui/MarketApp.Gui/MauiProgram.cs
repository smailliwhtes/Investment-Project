using MarketApp.Gui.Core;
using MarketApp.Gui.Pages;
using MarketApp.Gui.Services;

namespace MarketApp.Gui;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder.UseMauiApp<App>();

        builder.Services.AddSingleton<SampleDataService>();
        builder.Services.AddSingleton<SimulatedRunOrchestrator>();
        builder.Services.AddSingleton<ISecretsStore, SecureSecretsStore>();
        builder.Services.AddSingleton<IChartProvider, DefaultChartProvider>();

        builder.Services.AddSingleton<DashboardViewModel>();
        builder.Services.AddSingleton<RunViewModel>();
        builder.Services.AddSingleton<RunsViewModel>();
        builder.Services.AddSingleton<UniverseViewModel>();
        builder.Services.AddSingleton<SettingsViewModel>();
        builder.Services.AddSingleton<LogsViewModel>();

        builder.Services.AddSingleton<Pages.DashboardPage>();
        builder.Services.AddSingleton<Pages.RunPage>();
        builder.Services.AddSingleton<Pages.RunsPage>();
        builder.Services.AddSingleton<Pages.UniversePage>();
        builder.Services.AddSingleton<Pages.SettingsPage>();
        builder.Services.AddSingleton<Pages.LogsPage>();
        builder.Services.AddSingleton<AppShell>();

        return builder.Build();
    }
}
