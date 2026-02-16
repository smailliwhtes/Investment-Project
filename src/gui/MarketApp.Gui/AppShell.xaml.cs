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
            Title = "Dashboard",
            Items = { new ShellContent { Content = dashboardPage, Title = "Dashboard" } }
        });
        Items.Add(new FlyoutItem
        {
            Title = "Run",
            Items = { new ShellContent { Content = runPage, Title = "Run" } }
        });
        Items.Add(new FlyoutItem
        {
            Title = "Runs",
            Items = { new ShellContent { Content = runsPage, Title = "History" } }
        });
        Items.Add(new FlyoutItem
        {
            Title = "Universe",
            Items = { new ShellContent { Content = universePage, Title = "Universe" } }
        });
        Items.Add(new FlyoutItem
        {
            Title = "Settings",
            Items = { new ShellContent { Content = settingsPage, Title = "Settings" } }
        });
        Items.Add(new FlyoutItem
        {
            Title = "Logs",
            Items = { new ShellContent { Content = logsPage, Title = "Logs" } }
        });
    }
}
