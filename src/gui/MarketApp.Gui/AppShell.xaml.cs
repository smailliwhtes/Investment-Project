using MarketApp.Gui.Pages;

namespace MarketApp.Gui;

public partial class AppShell : Shell
{
    public AppShell(
        DashboardPage dashboardPage,
        RunPage runPage,
        RunsPage runsPage,
        UniversePage universePage,
        SettingsPage settingsPage,
        LogsPage logsPage)
    {
        InitializeComponent();

        Items.Add(new FlyoutItem
        {
            Route = "dashboard",
            Title = "Dashboard",
            Items = { new ShellContent { Route = "dashboard/home", Content = dashboardPage, Title = "Dashboard" } }
        });

        Items.Add(new FlyoutItem
        {
            Route = "run",
            Title = "Run",
            Items = { new ShellContent { Route = "run/orchestration", Content = runPage, Title = "Run Orchestration" } }
        });

        Items.Add(new FlyoutItem
        {
            Route = "runs",
            Title = "Runs",
            Items = { new ShellContent { Route = "runs/history", Content = runsPage, Title = "Runs History" } }
        });

        Items.Add(new FlyoutItem
        {
            Route = "analysis",
            Title = "Analysis",
            Items = { new ShellContent { Route = "analysis/universe", Content = universePage, Title = "Universe" } }
        });

        Items.Add(new FlyoutItem
        {
            Route = "settings",
            Title = "Settings",
            Items = { new ShellContent { Route = "settings/home", Content = settingsPage, Title = "Settings" } }
        });

        Items.Add(new FlyoutItem
        {
            Route = "logs",
            Title = "Logs",
            Items = { new ShellContent { Route = "logs/home", Content = logsPage, Title = "Logs" } }
        });
    }
}
