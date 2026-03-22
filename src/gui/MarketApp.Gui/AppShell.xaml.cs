using MarketApp.Gui.Pages;

namespace MarketApp.Gui;

public partial class AppShell : Shell
{
    public AppShell(
        DashboardPage dashboardPage,
        RunPage runPage,
        PolicySimulatorPage policySimulatorPage,
        RunsPage runsPage,
        UniversePage universePage,
        SettingsPage settingsPage,
        LogsPage logsPage)
    {
        InitializeComponent();

        var dashboardItem = CreateFlyoutItem(
            itemRoute: "dashboard_item",
            itemTitle: "Dashboard",
            contentRoute: "dashboard_home",
            contentTitle: "Dashboard",
            page: dashboardPage);

        var runItem = CreateFlyoutItem(
            itemRoute: "run_item",
            itemTitle: "Run",
            contentRoute: "run_orchestration",
            contentTitle: "Run Orchestration",
            page: runPage);

        var policyItem = CreateFlyoutItem(
            itemRoute: "policy_item",
            itemTitle: "Policy",
            contentRoute: "policy_simulator",
            contentTitle: "Policy Simulator",
            page: policySimulatorPage);

        var runsItem = CreateFlyoutItem(
            itemRoute: "runs_item",
            itemTitle: "Runs",
            contentRoute: "runs_history",
            contentTitle: "Runs History",
            page: runsPage);

        var analysisItem = CreateFlyoutItem(
            itemRoute: "analysis_item",
            itemTitle: "Analysis",
            contentRoute: "analysis_universe",
            contentTitle: "Universe",
            page: universePage);

        var settingsItem = CreateFlyoutItem(
            itemRoute: "settings_item",
            itemTitle: "Settings",
            contentRoute: "settings_home",
            contentTitle: "Settings",
            page: settingsPage);

        var logsItem = CreateFlyoutItem(
            itemRoute: "logs_item",
            itemTitle: "Logs",
            contentRoute: "logs_home",
            contentTitle: "Logs",
            page: logsPage);

        Items.Add(dashboardItem);
        Items.Add(runItem);
        Items.Add(policyItem);
        Items.Add(runsItem);
        Items.Add(analysisItem);
        Items.Add(settingsItem);
        Items.Add(logsItem);

        CurrentItem = dashboardItem;
        Navigated += OnShellNavigated;
    }

    private static FlyoutItem CreateFlyoutItem(string itemRoute, string itemTitle, string contentRoute, string contentTitle, Page page)
    {
        var content = new ShellContent
        {
            Route = contentRoute,
            Title = contentTitle,
            Content = page,
        };

        return new FlyoutItem
        {
            Route = itemRoute,
            Title = itemTitle,
            Items = { content },
        };
    }

    private void OnShellNavigated(object? sender, ShellNavigatedEventArgs e)
    {
        // Desktop QoL: close flyout after navigation so page changes are obvious.
        FlyoutIsPresented = false;
    }
}
