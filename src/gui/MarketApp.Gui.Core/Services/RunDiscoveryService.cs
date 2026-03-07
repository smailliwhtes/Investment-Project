using System.Globalization;
using System.Text.Json;

namespace MarketApp.Gui.Core;

public sealed class RunDiscoveryService : IRunDiscoveryService
{
    public Task<IReadOnlyList<RunSummary>> DiscoverRunsAsync(CancellationToken cancellationToken = default)
    {
        var outputsRoot = ResolveOutputsRoot();
        if (string.IsNullOrWhiteSpace(outputsRoot))
        {
            return Task.FromResult<IReadOnlyList<RunSummary>>(Array.Empty<RunSummary>());
        }

        var runsRoot = Path.Combine(outputsRoot, "runs");
        if (!Directory.Exists(runsRoot))
        {
            return Task.FromResult<IReadOnlyList<RunSummary>>(Array.Empty<RunSummary>());
        }

        var runs = new List<RunSummary>();
        foreach (var manifestPath in Directory.EnumerateFiles(runsRoot, "run_manifest.json", SearchOption.AllDirectories))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            try
            {
                var runDirectory = Path.GetDirectoryName(manifestPath);
                if (string.IsNullOrWhiteSpace(runDirectory))
                {
                    continue;
                }

                using var doc = JsonDocument.Parse(File.ReadAllText(manifestPath));
                var root = doc.RootElement;

                var runId = GetString(root, "run_id") ?? Path.GetFileName(runDirectory);
                var started = ParseDateTime(GetString(root, "started_at") ?? GetString(root, "started_utc"));
                var finished = ParseDateTime(GetString(root, "finished_at") ?? GetString(root, "started_at") ?? GetString(root, "started_utc"));

                var universe = GetNestedInt(root, "counts", "universe_count") ?? CountCsvRows(Path.Combine(runDirectory, "scored.csv"));
                var eligible = GetNestedInt(root, "counts", "eligible_count") ?? CountCsvRows(Path.Combine(runDirectory, "eligible.csv"));
                var worstLag = GetNestedInt(root, "data_freshness", "worst_lag_days") ?? 0;
                var medianLag = GetNestedDouble(root, "data_freshness", "median_lag_days") ?? 0.0;
                var lastDateMax = GetNestedString(root, "data_freshness", "last_date_max") ?? string.Empty;

                runs.Add(new RunSummary(
                    RunId: runId,
                    StartedAt: started,
                    FinishedAt: finished,
                    UniverseCount: universe,
                    EligibleCount: eligible,
                    WorstLagDays: worstLag,
                    MedianLagDays: medianLag,
                    LastDateMax: lastDateMax,
                    Status: "Completed",
                    RunDirectory: runDirectory));
            }
            catch
            {
                // Keep scanning if one run is malformed.
            }
        }

        var sorted = runs
            .OrderByDescending(r => r.FinishedAt)
            .ThenByDescending(r => r.StartedAt)
            .ToArray();

        return Task.FromResult<IReadOnlyList<RunSummary>>(sorted);
    }

    public string? ResolveOutputsRoot()
    {
        var env = Environment.GetEnvironmentVariable("MARKETAPP_OUTPUTS_DIR");
        if (!string.IsNullOrWhiteSpace(env) && Directory.Exists(env))
        {
            return Path.GetFullPath(env);
        }

        var current = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 12 && current is not null; i++)
        {
            var candidate = Path.Combine(current.FullName, "market_app", "outputs");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        return null;
    }

    private static int CountCsvRows(string path)
    {
        if (!File.Exists(path))
        {
            return 0;
        }

        var lineCount = File.ReadLines(path).Count();
        return Math.Max(lineCount - 1, 0);
    }

    private static DateTime ParseDateTime(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return DateTime.UtcNow;
        }

        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
        {
            return parsed.ToUniversalTime();
        }

        return DateTime.UtcNow;
    }

    private static string? GetString(JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private static int? GetInt(JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var value))
        {
            return null;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number))
        {
            return number;
        }

        if (value.ValueKind == JsonValueKind.String && int.TryParse(value.GetString(), out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static double? GetDouble(JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var value))
        {
            return null;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number))
        {
            return number;
        }

        if (value.ValueKind == JsonValueKind.String && double.TryParse(value.GetString(), out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static string? GetNestedString(JsonElement root, string parent, string child)
    {
        if (!root.TryGetProperty(parent, out var node) || node.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetString(node, child);
    }

    private static int? GetNestedInt(JsonElement root, string parent, string child)
    {
        if (!root.TryGetProperty(parent, out var node) || node.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetInt(node, child);
    }

    private static double? GetNestedDouble(JsonElement root, string parent, string child)
    {
        if (!root.TryGetProperty(parent, out var node) || node.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetDouble(node, child);
    }
}
