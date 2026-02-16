namespace MarketApp.Gui.Core.Models;

public enum ChartProviderKind
{
    LiveCharts2,
    Syncfusion,
    Telerik,
}

public sealed class AppSettings
{
    public string ActiveConfigPath { get; set; } = "market_app/config/config.yaml";
    public string OutputsRunsRoot { get; set; } = "market_app/outputs/runs";
    public string? PythonPathOverride { get; set; }
    public ChartProviderKind ChartProvider { get; set; } = ChartProviderKind.LiveCharts2;
}
