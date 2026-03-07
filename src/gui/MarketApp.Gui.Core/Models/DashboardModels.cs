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
    string Status,
    string RunDirectory = ""
);

public sealed record BacktestMetricRow(
    string Model,
    double? Mse,
    double? Mae,
    double? Accuracy,
    double? F1,
    int Splits
);

public sealed record CauseEffectSnapshot(
    int MarketRows,
    int GdeltRows,
    int JoinedRows,
    int EventImpactRows,
    string? TopContextDay,
    string? TopContextMetric,
    double? TopContextValue,
    DateTime? GeneratedAtUtc = null
);

public sealed record RunQualitySnapshot(
    string RunId,
    DateTime? EvaluatedAtUtc,
    IReadOnlyList<BacktestMetricRow> Metrics
);

public sealed record DashboardSummary(
    RunSummary LastRun,
    IReadOnlyList<ScoreRow> TopSymbols,
    IReadOnlyList<LogEntry> RecentLogs,
    CauseEffectSnapshot? CauseEffect,
    IReadOnlyList<BacktestMetricRow> BacktestMetrics,
    RunQualitySnapshot? QualitySnapshot = null
);

public sealed record DataFreshnessSummary(int WorstLagDays, double MedianLagDays, string LastDateMax);

public sealed record ProgressCounters(int Done, int Total, string Units);

public sealed record ProgressArtifact(string Name, string Path, int? Rows, string? Hash);

public sealed record ProgressError(string Code, string Detail, string? Traceback);

public sealed record ProgressEvent(
    string Type,
    string Stage,
    string Message,
    double? Pct,
    DateTime Timestamp,
    ProgressCounters? Counters = null,
    ProgressArtifact? Artifact = null,
    ProgressError? Error = null,
    string? RawLine = null
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

public sealed record RunDiffSummary(int NSymbols, int NNew, int NRemoved, int NRankChanged);

public sealed record RunDiffRow(
    string Symbol,
    int? RankA,
    int? RankB,
    double? ScoreA,
    double? ScoreB,
    double? DeltaScore,
    int? DeltaRank,
    int? FlagsA,
    int? FlagsB,
    IReadOnlyList<string> Drivers
);

public sealed record RunDiffResult(
    string RunA,
    string RunB,
    RunDiffSummary Summary,
    IReadOnlyList<RunDiffRow> Rows
);
