using System.Text.Json;
using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Core.Services;

public static class RunDiscoveryService
{
    public static IReadOnlyList<RunMetadata> DiscoverRuns(string outputsRunsRoot)
    {
        if (!Directory.Exists(outputsRunsRoot)) return Array.Empty<RunMetadata>();
        var runs = new List<RunMetadata>();
        foreach (var dir in Directory.GetDirectories(outputsRunsRoot))
        {
            var runId = Path.GetFileName(dir);
            var manifestPath = Path.Combine(dir, "run_manifest.json");
            string configHash = string.Empty;
            DateTime? ts = null;
            if (File.Exists(manifestPath))
            {
                using var doc = JsonDocument.Parse(File.ReadAllText(manifestPath));
                if (doc.RootElement.TryGetProperty("config_hash", out var hash)) configHash = hash.GetString() ?? string.Empty;
                if (doc.RootElement.TryGetProperty("started_utc", out var started) && DateTime.TryParse(started.GetString(), out var parsed)) ts = parsed;
            }
            runs.Add(new RunMetadata(runId, ts, configHash, 0, 0, string.Empty, 0));
        }
        return runs.OrderByDescending(r => r.TimestampUtc).ToList();
    }
}
