using System.Collections.ObjectModel;

namespace MarketApp.Gui.Core;

public class SampleDataService
{
    private readonly IReadOnlyList<ScoreRow> _scores;
    private readonly IReadOnlyList<RunSummary> _runHistory;
    private readonly IReadOnlyList<LogEntry> _logs;

    public SampleDataService()
    {
        _scores = BuildScores();
        _runHistory = BuildRunHistory();
        _logs = BuildLogs();
    }

    public DashboardSummary GetDashboard()
    {
        var lastRun = _runHistory.First();
        return new DashboardSummary(lastRun, _scores.Take(5).ToArray(), _logs.Take(4).ToArray());
    }

    public IReadOnlyList<RunSummary> GetRunHistory() => _runHistory;

    public IReadOnlyList<ScoreRow> GetScores() => _scores;

    public IReadOnlyList<LogEntry> GetLogs() => _logs;

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
                today.ToString("yyyy-MM-dd"),
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
