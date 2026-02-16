namespace MarketApp.Gui.Core;

public sealed record RunSummary(
    string RunId,
    DateTime StartedAt,
    DateTime FinishedAt,
    int UniverseCount,
    int EligibleCount,
    int WorstLagDays,
    double MedianLagDays,
    string LastDateMax,
    string Status
);

public sealed record DashboardSummary(
    RunSummary LastRun,
    IReadOnlyList<ScoreRow> TopSymbols,
    IReadOnlyList<LogEntry> RecentLogs
);

public sealed record DataFreshnessSummary(int WorstLagDays, double MedianLagDays, string LastDateMax);

public sealed record ProgressEvent(
    string Type,
    string Stage,
    string Message,
    double? Pct,
    DateTime Timestamp
);

public sealed record ScoreRow(
    string Symbol,
    double Score,
    int Rank,
    string GatesPassed,
    int FlagsCount,
    string ThemeLabels,
    string LastDate,
    int LagDays
);

public sealed record LogEntry(DateTime Timestamp, string Level, string Message);
