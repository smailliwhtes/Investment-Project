using CommunityToolkit.Maui;
using MarketApp.Gui.Core.Abstractions;
using MarketApp.Gui.Core.Services;
using MarketApp.Gui.Core.ViewModels;
using MarketApp.Gui.Views;
using MarketApp.Gui.Services;
using MarketApp.Gui.Controls;

namespace MarketApp.Gui;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder
            .UseMauiApp<App>()
            .UseMauiCommunityToolkit();

        builder.Services.AddSingleton<IEngineBridge, EngineBridgeService>();
        builder.Services.AddSingleton<ISecretStore, MauiSecretStore>();
        builder.Services.AddSingleton<IChartProvider, LiveChartsChartProvider>();
        builder.Services.AddSingleton<MainViewModel>();
        builder.Services.AddSingleton<AppShell>();
        builder.Services.AddTransient<DashboardPage>();
        builder.Services.AddTransient<RunsPage>();
        builder.Services.AddTransient<SettingsPage>();
        builder.Services.AddTransient<LogsPage>();

        return builder.Build();
    }
}
