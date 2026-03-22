using System.Text;
using System.Text.Json;

namespace MarketApp.Gui.Core;

public class PolicySimulatorViewModel : ViewModelBase
{
    private readonly IEngineBridgeService _engineBridge;
    private readonly IUserSettingsService _settings;
    private CancellationTokenSource? _cts;
    private string _configPath = "config/config.yaml";
    private string _scenarioName = "tariff-shock";
    private string _outputDirectory = "outputs/policy_simulations/local";
    private string _pythonPath;
    private string _status = "Idle";
    private string _summaryText = "Run a scenario to see the policy simulation summary.";
    private string? _summaryPath;
    private bool _isRunning;

    public PolicySimulatorViewModel(IEngineBridgeService engineBridge, IUserSettingsService settings)
    {
        _engineBridge = engineBridge;
        _settings = settings;
        _pythonPath = _settings.GetPythonPath() ?? "python";

        Title = "Policy Simulator";
        RunCommand = new AsyncRelayCommand(RunSimulationAsync, () => !IsRunning);
        CancelCommand = new RelayCommand(CancelRun, () => IsRunning);
    }

    public string ConfigPath
    {
        get => _configPath;
        set => SetProperty(ref _configPath, value);
    }

    public string ScenarioName
    {
        get => _scenarioName;
        set => SetProperty(ref _scenarioName, value);
    }

    public string OutputDirectory
    {
        get => _outputDirectory;
        set => SetProperty(ref _outputDirectory, value);
    }

    public string PythonPath
    {
        get => _pythonPath;
        set
        {
            if (SetProperty(ref _pythonPath, value))
            {
                _settings.SetPythonPath(value);
            }
        }
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string SummaryText
    {
        get => _summaryText;
        set => SetProperty(ref _summaryText, value);
    }

    public string? SummaryPath
    {
        get => _summaryPath;
        set => SetProperty(ref _summaryPath, value);
    }

    public bool IsRunning
    {
        get => _isRunning;
        set
        {
            if (SetProperty(ref _isRunning, value))
            {
                RunCommand.RaiseCanExecuteChanged();
                CancelCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public AsyncRelayCommand RunCommand { get; }
    public RelayCommand CancelCommand { get; }

    internal async Task RunSimulationAsync()
    {
        _cts?.Dispose();
        _cts = new CancellationTokenSource();
        IsRunning = true;
        Status = "Running policy simulation";
        SummaryText = "Waiting for simulation output...";
        SummaryPath = null;

        try
        {
            var request = new PolicySimulationRequest(
                ConfigPath: ConfigPath,
                ScenarioName: ScenarioName,
                OutDirectory: OutputDirectory,
                Offline: true,
                PythonPath: string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath);

            var progressLines = new List<string>();
            var result = new EngineCommandResult(0, string.Empty, string.Empty);
            await foreach (var evt in _engineBridge.SimulatePolicyStreamAsync(request, _cts.Token))
            {
                var progressLine = FormatProgressLine(evt);
                progressLines.Add(progressLine);
                if (progressLines.Count > 200)
                {
                    progressLines.RemoveAt(0);
                }

                SummaryText = string.Join(Environment.NewLine, progressLines);
                Status = BuildLiveStatus(evt);

                if (IsTerminalPolicyEvent(evt))
                {
                    var exitCode = evt.Type.Equals("stage_end", StringComparison.OrdinalIgnoreCase)
                        ? 0
                        : evt.Error?.Code.Equals("INTERRUPTED", StringComparison.OrdinalIgnoreCase) == true
                            ? 130
                            : 4;

                    var errorText = evt.Error?.Detail ?? string.Empty;
                    result = new EngineCommandResult(exitCode, string.Empty, errorText);
                }
            }

            var summary = TryLoadSummaryFromDirectory(OutputDirectory);
            if (summary is not null)
            {
                SummaryPath = summary.SummaryPath;
                SummaryText = BuildSummaryText(result, summary, progressLines);
                Status = result.ExitCode == 0
                    ? "Policy simulation complete"
                    : "Policy simulation completed with errors";
            }
            else
            {
                SummaryText = BuildSummaryText(result, null, progressLines);
                Status = result.ExitCode == 0
                    ? "Policy simulation complete"
                    : "Policy simulation failed";
            }
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
            SummaryText = "The policy simulation was canceled before completion.";
        }
        finally
        {
            IsRunning = false;
            _cts?.Dispose();
            _cts = null;
        }
    }

    private void CancelRun()
    {
        _cts?.Cancel();
    }

    internal static PolicySimulationSummary? TryLoadSummaryFromDirectory(string outDirectory)
    {
        if (string.IsNullOrWhiteSpace(outDirectory))
        {
            return null;
        }

        var directory = Path.GetFullPath(outDirectory);
        if (!Directory.Exists(directory))
        {
            return null;
        }

        foreach (var fileName in EngineBridgeService.PolicySummaryFileNames)
        {
            var candidate = Path.Combine(directory, fileName);
            if (!File.Exists(candidate))
            {
                continue;
            }

            var summary = TryLoadSummaryFromFile(candidate);
            if (summary is not null)
            {
                return summary;
            }
        }

        return null;
    }

    internal static PolicySimulationSummary? TryLoadSummaryFromFile(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        var rawJson = File.ReadAllText(path, System.Text.Encoding.UTF8);
        if (string.IsNullOrWhiteSpace(rawJson))
        {
            return new PolicySimulationSummary(null, null, null, null, Array.Empty<PolicySimulationSummaryField>(), rawJson, path);
        }

        try
        {
            using var doc = JsonDocument.Parse(rawJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return new PolicySimulationSummary(null, null, null, null, Array.Empty<PolicySimulationSummaryField>(), rawJson, path);
            }

            var fields = new List<PolicySimulationSummaryField>();
            if (root.TryGetProperty("fields", out var fieldsNode) && fieldsNode.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in fieldsNode.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    var name = GetString(item, "name") ?? GetString(item, "label") ?? GetString(item, "key");
                    var value = GetString(item, "value") ?? GetString(item, "text");
                    if (!string.IsNullOrWhiteSpace(name) && value is not null)
                    {
                        fields.Add(new PolicySimulationSummaryField(name, value));
                    }
                }
            }

            foreach (var property in root.EnumerateObject())
            {
                if (property.NameEquals("fields") ||
                    property.NameEquals("scenario") ||
                    property.NameEquals("status") ||
                    property.NameEquals("summary") ||
                    property.NameEquals("output_dir") ||
                    property.NameEquals("outdir") ||
                    property.NameEquals("output_directory"))
                {
                    continue;
                }

                if (TryGetScalarText(property.Value, out var value))
                {
                    fields.Add(new PolicySimulationSummaryField(property.Name, value));
                }
            }

            return new PolicySimulationSummary(
                Scenario: GetString(root, "scenario"),
                Status: GetString(root, "status"),
                Summary: GetString(root, "summary"),
                OutputDirectory: GetString(root, "output_dir") ?? GetString(root, "outdir") ?? GetString(root, "output_directory"),
                Fields: fields,
                RawJson: rawJson,
                SummaryPath: path);
        }
        catch (JsonException)
        {
            return new PolicySimulationSummary(null, null, null, null, Array.Empty<PolicySimulationSummaryField>(), rawJson, path);
        }
    }

    internal static string BuildSummaryText(
        EngineCommandResult result,
        PolicySimulationSummary? summary,
        IReadOnlyList<string>? progressLines = null)
    {
        var lines = new List<string>();
        lines.Add(summary?.Summary ?? "No JSON summary file was found.");

        if (summary is not null)
        {
            if (!string.IsNullOrWhiteSpace(summary.Scenario))
            {
                lines.Add($"Scenario: {summary.Scenario}");
            }
            if (!string.IsNullOrWhiteSpace(summary.Status))
            {
                lines.Add($"Status: {summary.Status}");
            }
            if (!string.IsNullOrWhiteSpace(summary.OutputDirectory))
            {
                lines.Add($"Output directory: {summary.OutputDirectory}");
            }
            if (summary.Fields.Count > 0)
            {
                lines.Add(string.Empty);
                lines.Add("Fields:");
                foreach (var field in summary.Fields)
                {
                    lines.Add($"- {field.Name}: {field.Value}");
                }
            }
            lines.Add(string.Empty);
            lines.Add("Raw JSON:");
            lines.Add(summary.RawJson.Trim());
        }
        else
        {
            if (!string.IsNullOrWhiteSpace(result.Stdout))
            {
                lines.Add(string.Empty);
                lines.Add("Stdout:");
                lines.Add(result.Stdout.Trim());
            }
            if (!string.IsNullOrWhiteSpace(result.Stderr))
            {
                lines.Add(string.Empty);
                lines.Add("Stderr:");
                lines.Add(result.Stderr.Trim());
            }
        }

        if (progressLines is not null && progressLines.Count > 0)
        {
            lines.Add(string.Empty);
            lines.Add("Progress log:");
            foreach (var line in progressLines)
            {
                lines.Add(line);
            }
        }

        return string.Join(Environment.NewLine, lines.Where(line => line is not null));
    }

    private static bool IsTerminalPolicyEvent(ProgressEvent evt)
    {
        if (!evt.Stage.Equals("policy_simulate", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return evt.Type.Equals("stage_end", StringComparison.OrdinalIgnoreCase)
            || evt.Type.Equals("error", StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildLiveStatus(ProgressEvent evt)
    {
        if (evt.Pct.HasValue)
        {
            return $"{evt.Message} ({evt.Pct:P0})";
        }

        return evt.Message;
    }

    private static string FormatProgressLine(ProgressEvent evt)
    {
        var builder = new StringBuilder();
        builder.Append('[');
        builder.Append(evt.Timestamp.ToLocalTime().ToString("HH:mm:ss"));
        builder.Append("] ");
        builder.Append(evt.Stage);
        builder.Append('/');
        builder.Append(evt.Type);
        if (evt.Pct.HasValue)
        {
            builder.Append(' ');
            builder.Append(evt.Pct.Value.ToString("P0"));
        }
        builder.Append(": ");
        builder.Append(evt.Message);

        if (!string.IsNullOrWhiteSpace(evt.Error?.Detail))
        {
            builder.Append(" | ");
            builder.Append(evt.Error.Detail);
        }

        return builder.ToString();
    }

    private static bool TryGetScalarText(JsonElement node, out string value)
    {
        value = string.Empty;
        switch (node.ValueKind)
        {
            case JsonValueKind.String:
                value = node.GetString() ?? string.Empty;
                return true;
            case JsonValueKind.Number:
            case JsonValueKind.True:
            case JsonValueKind.False:
                value = node.GetRawText();
                return true;
            default:
                return false;
        }
    }

    private static string? GetString(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var node))
        {
            return null;
        }

        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Number => node.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }
}
