using System.Globalization;
using System.Text.Json;

namespace MarketApp.Gui.Core;

public class SampleDataService
{
    private readonly IReadOnlyList<ScoreRow> _scores;
    private readonly IReadOnlyList<RunSummary> _runHistory;
    private readonly IReadOnlyList<LogEntry> _logs;
    private readonly CauseEffectSnapshot? _causeEffect;
    private readonly IReadOnlyList<BacktestMetricRow> _backtestMetrics;

    public SampleDataService()
    {
        if (TryLoadFromArtifacts(
            out var scores,
            out var runHistory,
            out var logs,
            out var causeEffect,
            out var backtestMetrics))
        {
            _scores = scores;
            _runHistory = runHistory;
            _logs = logs;
            _causeEffect = causeEffect;
            _backtestMetrics = backtestMetrics;
            return;
        }

        _scores = BuildScores();
        _runHistory = BuildRunHistory();
        _logs = BuildLogs();
        _causeEffect = new CauseEffectSnapshot(
            MarketRows: 1200,
            GdeltRows: 365,
            JoinedRows: 1080,
            EventImpactRows: 240,
            TopContextDay: "2025-01-27",
            TopContextMetric: "conflict_event_count_total",
            TopContextValue: 42);
        _backtestMetrics = new[]
        {
            new BacktestMetricRow("market_only", 0.012, 0.079, 0.57, 0.52, 3),
            new BacktestMetricRow("market_plus_corpus", 0.010, 0.072, 0.61, 0.56, 3)
        };
    }

    public DashboardSummary GetDashboard()
    {
        var lastRun = _runHistory.First();
        var qualitySnapshot = _backtestMetrics.Count > 0
            ? new RunQualitySnapshot(lastRun.RunId, lastRun.FinishedAt, _backtestMetrics)
            : null;

        return new DashboardSummary(
            lastRun,
            _scores.Take(10).ToArray(),
            _logs.Take(8).ToArray(),
            _causeEffect,
            _backtestMetrics,
            qualitySnapshot);
    }

    public IReadOnlyList<RunSummary> GetRunHistory() => _runHistory;

    public IReadOnlyList<ScoreRow> GetScores() => _scores;

    public IReadOnlyList<LogEntry> GetLogs() => _logs;

    private static bool TryLoadFromArtifacts(
        out IReadOnlyList<ScoreRow> scores,
        out IReadOnlyList<RunSummary> runHistory,
        out IReadOnlyList<LogEntry> logs,
        out CauseEffectSnapshot? causeEffect,
        out IReadOnlyList<BacktestMetricRow> backtestMetrics)
    {
        scores = Array.Empty<ScoreRow>();
        runHistory = Array.Empty<RunSummary>();
        logs = Array.Empty<LogEntry>();
        causeEffect = null;
        backtestMetrics = Array.Empty<BacktestMetricRow>();

        var outputsRoot = ResolveOutputsRoot();
        if (string.IsNullOrWhiteSpace(outputsRoot) || !Directory.Exists(outputsRoot))
        {
            return false;
        }

        var loadedRuns = LoadRuns(outputsRoot);
        if (loadedRuns.Count == 0)
        {
            return false;
        }

        runHistory = loadedRuns.Select(r => r.Summary).ToArray();
        var latest = loadedRuns[0];
        scores = LoadScores(latest.RunDirectory);
        logs = LoadLogs(latest.RunDirectory);
        causeEffect = LoadCauseEffect(latest.RunDirectory, outputsRoot);
        backtestMetrics = LoadBacktestMetrics(latest.RunDirectory, outputsRoot);
        return scores.Count > 0;
    }

    private static string? ResolveOutputsRoot()
    {
        var env = Environment.GetEnvironmentVariable("MARKETAPP_OUTPUTS_DIR");
        if (!string.IsNullOrWhiteSpace(env))
        {
            return Path.GetFullPath(env);
        }

        var current = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 8 && current is not null; i++)
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

    private sealed record LoadedRun(RunSummary Summary, string RunDirectory);

    private static IReadOnlyList<LoadedRun> LoadRuns(string outputsRoot)
    {
        var runsDir = Path.Combine(outputsRoot, "runs");
        if (!Directory.Exists(runsDir))
        {
            return Array.Empty<LoadedRun>();
        }

        var loaded = new List<LoadedRun>();
        foreach (var manifestPath in Directory.EnumerateFiles(runsDir, "run_manifest.json", SearchOption.AllDirectories))
        {
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
                var medianLag = GetNestedDouble(root, "data_freshness", "median_lag_days") ?? 0;
                var lastDateMax = GetNestedString(root, "data_freshness", "last_date_max") ?? string.Empty;

                loaded.Add(new LoadedRun(
                    new RunSummary(
                        runId,
                        started,
                        finished,
                        universe,
                        eligible,
                        worstLag,
                        medianLag,
                        lastDateMax,
                        "Completed",
                        runDirectory),
                    runDirectory));
            }
            catch
            {
                // Ignore malformed manifests and continue scanning.
            }
        }

        return loaded
            .OrderByDescending(r => r.Summary.FinishedAt)
            .ToArray();
    }

    private static IReadOnlyList<ScoreRow> LoadScores(string runDirectory)
    {
        var scoredPath = Path.Combine(runDirectory, "scored.csv");
        if (!File.Exists(scoredPath))
        {
            return Array.Empty<ScoreRow>();
        }

        var rows = ReadCsvRows(scoredPath);
        if (rows.Count == 0)
        {
            return Array.Empty<ScoreRow>();
        }

        var parsed = new List<ScoreRow>(rows.Count);
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var symbol = GetValue(row, "symbol") ?? string.Empty;
            if (string.IsNullOrWhiteSpace(symbol))
            {
                continue;
            }

            var score = ParseDouble(
                GetValue(row, "score")
                ?? GetValue(row, "score_1to10")
                ?? GetValue(row, "priority_score")
                ?? GetValue(row, "monitor_score_1_10"));
            var rank = ParseInt(GetValue(row, "rank")) ?? (i + 1);
            var gates = GetValue(row, "gates_passed") ?? "unknown";
            var flagsCount = ParseInt(GetValue(row, "flags_count"))
                ?? CountPipeItems(GetValue(row, "risk_flags"));
            var themeLabels = GetValue(row, "theme_labels")
                ?? GetValue(row, "theme_bucket")
                ?? string.Empty;
            var lastDate = GetValue(row, "last_date") ?? string.Empty;
            var lagDays = ParseInt(GetValue(row, "lag_days")) ?? 0;

            parsed.Add(new ScoreRow(
                symbol.ToUpperInvariant(),
                score,
                rank,
                gates,
                flagsCount,
                themeLabels,
                lastDate,
                lagDays));
        }

        return parsed
            .OrderBy(s => s.Rank)
            .ThenBy(s => s.Symbol, StringComparer.Ordinal)
            .ToArray();
    }

    private static IReadOnlyList<LogEntry> LoadLogs(string runDirectory)
    {
        var candidates = new[]
        {
            Path.Combine(runDirectory, "ui_engine.log"),
            Path.Combine(runDirectory, "logs", "engine.log")
        };

        var entries = new List<LogEntry>();
        foreach (var path in candidates)
        {
            if (!File.Exists(path))
            {
                continue;
            }

            var lines = File.ReadAllLines(path)
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .TakeLast(120)
                .ToArray();
            var baseTime = File.GetLastWriteTimeUtc(path);
            for (var i = 0; i < lines.Length; i++)
            {
                var timestamp = baseTime.AddSeconds(i - lines.Length);
                entries.Add(new LogEntry(timestamp, InferLogLevel(lines[i]), lines[i].Trim()));
            }
        }

        if (entries.Count == 0)
        {
            return Array.Empty<LogEntry>();
        }

        return entries
            .OrderByDescending(e => e.Timestamp)
            .ToArray();
    }

    private static CauseEffectSnapshot? LoadCauseEffect(string runDirectory, string outputsRoot)
    {
        var summaryPath = Path.Combine(runDirectory, "corpus_linked", "cause_effect_summary.json");
        if (!File.Exists(summaryPath))
        {
            summaryPath = Path.Combine(outputsRoot, "corpus_linked", "cause_effect_summary.json");
        }
        if (!File.Exists(summaryPath))
        {
            return null;
        }

        using var doc = JsonDocument.Parse(File.ReadAllText(summaryPath));
        var root = doc.RootElement;
        if (!root.TryGetProperty("counts", out var counts))
        {
            return null;
        }

        var marketRows = GetInt(counts, "market_rows") ?? 0;
        var gdeltRows = GetInt(counts, "gdelt_rows") ?? 0;
        var joinedRows = GetInt(counts, "joined_rows") ?? 0;
        var eventImpactRows = GetInt(counts, "event_impact_rows") ?? 0;

        string? topDay = null;
        string? topMetric = null;
        double? topValue = null;
        if (root.TryGetProperty("top_context_days", out var top) && top.ValueKind == JsonValueKind.Array)
        {
            var first = top.EnumerateArray().FirstOrDefault();
            if (first.ValueKind == JsonValueKind.Object)
            {
                topDay = GetString(first, "day");
                topMetric = GetString(first, "metric");
                topValue = GetDouble(first, "value");
            }
        }

        return new CauseEffectSnapshot(
            marketRows,
            gdeltRows,
            joinedRows,
            eventImpactRows,
            topDay,
            topMetric,
            topValue,
            File.GetLastWriteTimeUtc(summaryPath));
    }

    private static IReadOnlyList<BacktestMetricRow> LoadBacktestMetrics(string runDirectory, string outputsRoot)
    {
        var metricsPath = Path.Combine(runDirectory, "eval", "eval_metrics.csv");
        if (!File.Exists(metricsPath))
        {
            metricsPath = Path.Combine(outputsRoot, "eval", "eval_metrics.csv");
        }
        if (!File.Exists(metricsPath))
        {
            return Array.Empty<BacktestMetricRow>();
        }

        var rows = ReadCsvRows(metricsPath);
        if (rows.Count == 0)
        {
            return Array.Empty<BacktestMetricRow>();
        }

        var grouped = rows
            .GroupBy(r => (GetValue(r, "model") ?? "unknown").Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(group => new BacktestMetricRow(
                group.Key,
                Average(group, "mse"),
                Average(group, "mae"),
                Average(group, "accuracy"),
                Average(group, "f1"),
                group.Count()))
            .OrderBy(g => g.Model, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return grouped;
    }

    private static double? Average(IEnumerable<Dictionary<string, string>> rows, string key)
    {
        var values = rows
            .Select(r => ParseNullableDouble(GetValue(r, key)))
            .Where(v => v.HasValue)
            .Select(v => v!.Value)
            .ToArray();
        if (values.Length == 0)
        {
            return null;
        }

        return values.Average();
    }

    private static IReadOnlyList<Dictionary<string, string>> ReadCsvRows(string path)
    {
        var lines = File.ReadAllLines(path);
        if (lines.Length <= 1)
        {
            return Array.Empty<Dictionary<string, string>>();
        }

        var headers = SplitCsvLine(lines[0]);
        var rows = new List<Dictionary<string, string>>();
        foreach (var line in lines.Skip(1))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = SplitCsvLine(line);
            var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < headers.Count; i++)
            {
                var value = i < values.Count ? values[i] : string.Empty;
                row[headers[i]] = value;
            }
            rows.Add(row);
        }

        return rows;
    }

    private static List<string> SplitCsvLine(string line)
    {
        var values = new List<string>();
        var current = new System.Text.StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var ch = line[i];
            if (ch == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (ch == ',' && !inQuotes)
            {
                values.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        values.Add(current.ToString());
        return values;
    }

    private static string? GetValue(Dictionary<string, string> row, string key)
    {
        return row.TryGetValue(key, out var value) ? value : null;
    }

    private static int CountPipeItems(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return 0;
        }

        return value.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Length;
    }

    private static int CountCsvRows(string path)
    {
        if (!File.Exists(path))
        {
            return 0;
        }

        var lines = File.ReadLines(path).Where(l => !string.IsNullOrWhiteSpace(l)).Count();
        return Math.Max(lines - 1, 0);
    }

    private static string InferLogLevel(string line)
    {
        if (line.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            return "error";
        }

        if (line.Contains("warn", StringComparison.OrdinalIgnoreCase))
        {
            return "warn";
        }

        return "info";
    }

    private static DateTime ParseDateTime(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return DateTime.UtcNow;
        }

        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var parsed))
        {
            return DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
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

        if (value.ValueKind == JsonValueKind.String && double.TryParse(
                value.GetString(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static string? GetNestedString(JsonElement root, string first, string second)
    {
        if (!root.TryGetProperty(first, out var nested) || nested.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetString(nested, second);
    }

    private static int? GetNestedInt(JsonElement root, string first, string second)
    {
        if (!root.TryGetProperty(first, out var nested) || nested.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetInt(nested, second);
    }

    private static double? GetNestedDouble(JsonElement root, string first, string second)
    {
        if (!root.TryGetProperty(first, out var nested) || nested.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        return GetDouble(nested, second);
    }

    private static int? ParseInt(string? value)
    {
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static double ParseDouble(string? value)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return 0;
    }

    private static double? ParseNullableDouble(string? value)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static IReadOnlyList<ScoreRow> BuildScores()
    {
        var symbols = new[]
        {
            ("AAPL", 0.92, 1, 2, "Momentum;AI", 1),
            ("MSFT", 0.89, 2, 1, "Cloud;AI", 2),
            ("NVDA", 0.88, 3, 3, "AI;Semis", 3),
            ("AMZN", 0.86, 4, 0, "E-commerce;Cloud", 1),
            ("GOOG", 0.85, 5, 1, "Search;Cloud", 1),
            ("TSLA", 0.82, 6, 4, "EV;Energy", 5),
            ("JPM", 0.75, 7, 0, "Finance", 2),
            ("XOM", 0.73, 8, 1, "Energy", 4),
            ("KO", 0.65, 9, 0, "Consumer", 1),
            ("MCD", 0.64, 10, 0, "Consumer", 1)
        };

        var today = new DateTime(2025, 1, 31);
        return symbols
            .Select(s => new ScoreRow(
                s.Item1,
                s.Item2,
                s.Item3,
                "yes",
                s.Item4,
                s.Item5,
                today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                s.Item6))
            .ToArray();
    }

    private static IReadOnlyList<RunSummary> BuildRunHistory()
    {
        return new[]
        {
            new RunSummary(
                "run_2025_01_31",
                new DateTime(2025, 1, 31, 8, 0, 0, DateTimeKind.Utc),
                new DateTime(2025, 1, 31, 8, 4, 0, DateTimeKind.Utc),
                150,
                78,
                5,
                1.5,
                "2025-01-31",
                "Completed"),
            new RunSummary(
                "run_2025_01_30",
                new DateTime(2025, 1, 30, 8, 0, 0, DateTimeKind.Utc),
                new DateTime(2025, 1, 30, 8, 3, 0, DateTimeKind.Utc),
                149,
                75,
                6,
                1.8,
                "2025-01-30",
                "Completed"),
            new RunSummary(
                "run_2025_01_29",
                new DateTime(2025, 1, 29, 8, 0, 0, DateTimeKind.Utc),
                new DateTime(2025, 1, 29, 8, 2, 0, DateTimeKind.Utc),
                149,
                74,
                7,
                2.0,
                "2025-01-29",
                "Completed")
        };
    }

    private static IReadOnlyList<LogEntry> BuildLogs()
    {
        var start = new DateTime(2025, 1, 31, 8, 0, 0, DateTimeKind.Utc);
        return new[]
        {
            new LogEntry(start, "info", "Starting run pipeline with offline data"),
            new LogEntry(start.AddSeconds(30), "info", "Loaded OHLCV cache"),
            new LogEntry(start.AddMinutes(1), "info", "Scored 150 symbols"),
            new LogEntry(start.AddMinutes(3), "info", "Finished run; writing artifacts"),
            new LogEntry(start.AddMinutes(3).AddSeconds(30), "info", "Artifacts available under outputs/runs/run_2025_01_31")
        };
    }
}




