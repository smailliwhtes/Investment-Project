namespace MarketApp.Gui.Core;

public sealed record PolicySimulationRequest(
    string ConfigPath,
    string ScenarioName,
    string OutDirectory,
    bool Offline = true,
    string? PythonPath = null);

public sealed record PolicySimulationSummaryField(string Name, string Value);

public sealed record PolicySimulationSummary(
    string? Scenario,
    string? Status,
    string? Summary,
    string? OutputDirectory,
    IReadOnlyList<PolicySimulationSummaryField> Fields,
    string RawJson,
    string? SummaryPath = null);
