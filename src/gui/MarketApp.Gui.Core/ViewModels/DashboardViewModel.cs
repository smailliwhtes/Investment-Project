using System.Globalization;

namespace MarketApp.Gui.Core;

public class DashboardViewModel : ViewModelBase
{
    private static readonly string[] DefaultTrainingDeskItems =
    {
        "Start with the score, then check how old the data is.",
        "Use small position sizes while you are still practicing.",
        "Test a what-if before you trust a market story.",
    };

    private static readonly string[] DefaultRiskRules =
    {
        "Keep each trade small enough that one bad call does not hurt much.",
        "Older data means weaker confidence.",
        "A strong score is a clue, not a promise.",
        "Options can move fast, so decide your exit before you enter.",
    };

    private DashboardSummary? _summary;
    private IReadOnlyList<DashboardMetricTile> _overviewTiles = Array.Empty<DashboardMetricTile>();
    private IReadOnlyList<DashboardFact> _signalFacts = Array.Empty<DashboardFact>();
    private IReadOnlyList<DashboardFact> _scenarioFacts = Array.Empty<DashboardFact>();
    private IReadOnlyList<DashboardCallout> _quickHelp = Array.Empty<DashboardCallout>();
    private PriceSeriesModel _trendPreview = EmptyPriceSeries();
    private ForecastOverlayModel? _forecastPreview;
    private IndicatorSeriesModel _indicatorPreview = EmptyIndicatorSeries();
    private string _headerSubtitle = "Offline practice mode is ready.";
    private string _leadSymbol = "WATCH";
    private string _leadHeadline = "Top idea from the latest view";
    private string _leadScoreText = "Fit score: n/a";
    private string _leadLagText = "Newest data: n/a";
    private string _leadThemesText = "Main themes: n/a";
    private string _marketHeadline = "Load a run to see the latest market context.";
    private string _sentimentSummary = "Tone: Waiting for a completed run.";
    private string _watchlistCaption = "Top ideas from the latest completed run.";
    private string _bestModelDetail = "Model check details are not available yet.";

    public DashboardViewModel(SampleDataService dataService)
    {
        Title = "Dashboard";
        Summary = dataService.GetDashboard();
    }

    public DashboardSummary? Summary
    {
        get => _summary;
        set
        {
            if (!SetProperty(ref _summary, value))
            {
                return;
            }

            RefreshDerivedState();
            NotifyDashboardBindingsChanged();
        }
    }

    public RunSummary? LastRun => _summary?.LastRun;
    public IReadOnlyList<ScoreRow> TopSymbols => _summary?.TopSymbols ?? Array.Empty<ScoreRow>();
    public IReadOnlyList<LogEntry> RecentLogs => _summary?.RecentLogs ?? Array.Empty<LogEntry>();
    public CauseEffectSnapshot? CauseEffect => _summary?.CauseEffect;
    public IReadOnlyList<BacktestMetricRow> BacktestMetrics =>
        _summary?.BacktestMetrics ?? Array.Empty<BacktestMetricRow>();
    public RunQualitySnapshot? QualitySnapshot => _summary?.QualitySnapshot;
    public DataSourceSummary? DataSource => _summary?.DataSource;
    public DataReadinessSummary? DataReadiness => _summary?.DataReadiness;

    public string HeaderTitle => "Investment Dashboard";
    public string HeaderSubtitle => _headerSubtitle;
    public bool ShowDataSourceBanner => DataSource is not null;
    public string DataSourceTitle => DataSource?.Title ?? string.Empty;
    public string DataSourceMessage => DataSource?.Message ?? string.Empty;
    public IReadOnlyList<DataReadinessItem> DataReadinessItems =>
        DataReadiness?.Items ?? Array.Empty<DataReadinessItem>();
    public string DataReadinessTitle => DataReadiness?.Title ?? "What your data can do";
    public string DataReadinessMessage => DataReadiness?.Message ?? string.Empty;
    public IReadOnlyList<DashboardMetricTile> OverviewTiles => _overviewTiles;
    public IReadOnlyList<DashboardMetricTile> MarketOverviewTiles => _overviewTiles;
    public IReadOnlyList<string> TrainingDeskItems => DefaultTrainingDeskItems;
    public IReadOnlyList<string> TrainingChecklist => DefaultTrainingDeskItems;
    public IReadOnlyList<string> RiskRules => DefaultRiskRules;
    public IReadOnlyList<DashboardFact> SignalFacts => _signalFacts;
    public IReadOnlyList<DashboardFact> ScenarioFacts => _scenarioFacts;
    public IReadOnlyList<DashboardCallout> QuickHelp => _quickHelp;
    public string LeadSymbol => _leadSymbol;
    public string LeadHeadline => _leadHeadline;
    public string LeadScoreText => _leadScoreText;
    public string LeadLagText => _leadLagText;
    public string LeadThemesText => _leadThemesText;
    public string MarketHeadline => _marketHeadline;
    public string SentimentSummary => _sentimentSummary;
    public string WatchlistCaption => _watchlistCaption;
    public string BestModelDetail => _bestModelDetail;
    public PriceSeriesModel PricePreview => _trendPreview;
    public PriceSeriesModel TrendPreview => _trendPreview;
    public ForecastOverlayModel? ForecastPreview => _forecastPreview;
    public IndicatorSeriesModel IndicatorPreview => _indicatorPreview;

    private void RefreshDerivedState()
    {
        _overviewTiles = BuildOverviewTiles(_summary);
        _signalFacts = BuildSignalFacts(_summary);
        _scenarioFacts = BuildScenarioFacts(_summary);
        _quickHelp = BuildQuickHelp();
        (_trendPreview, _forecastPreview, _indicatorPreview) = BuildVisualModels(_summary);
        _headerSubtitle = BuildHeaderSubtitle(_summary);
        _marketHeadline = BuildMarketHeadline(_summary);
        _sentimentSummary = BuildSentimentSummary(_summary);
        _watchlistCaption = BuildWatchlistCaption(_summary);
        _bestModelDetail = BuildBestModelDetail(_summary);

        var lead = TopSymbols.FirstOrDefault();
        if (lead is null)
        {
            _leadSymbol = "WATCH";
            _leadHeadline = "Top idea from the latest view";
            _leadScoreText = "Fit score: n/a";
            _leadLagText = "Newest data: n/a";
            _leadThemesText = "Main themes: n/a";
            return;
        }

        _leadSymbol = lead.Symbol;
        _leadHeadline = lead.GatesPassed.Equals("yes", StringComparison.OrdinalIgnoreCase)
            ? "Passed the main checks"
            : "Worth watching with care";
        _leadScoreText = $"Fit score: {lead.Score:F2}";
        _leadLagText = $"Newest data: {FormatLagText(lead.LagDays)}";
        _leadThemesText = $"Main themes: {FormatFriendlyList(lead.ThemeLabels, "No theme tags yet")}";
    }

    private void NotifyDashboardBindingsChanged()
    {
        OnPropertyChanged(nameof(LastRun));
        OnPropertyChanged(nameof(TopSymbols));
        OnPropertyChanged(nameof(RecentLogs));
        OnPropertyChanged(nameof(CauseEffect));
        OnPropertyChanged(nameof(BacktestMetrics));
        OnPropertyChanged(nameof(QualitySnapshot));
        OnPropertyChanged(nameof(DataSource));
        OnPropertyChanged(nameof(DataReadiness));
        OnPropertyChanged(nameof(HeaderTitle));
        OnPropertyChanged(nameof(HeaderSubtitle));
        OnPropertyChanged(nameof(ShowDataSourceBanner));
        OnPropertyChanged(nameof(DataSourceTitle));
        OnPropertyChanged(nameof(DataSourceMessage));
        OnPropertyChanged(nameof(DataReadinessItems));
        OnPropertyChanged(nameof(DataReadinessTitle));
        OnPropertyChanged(nameof(DataReadinessMessage));
        OnPropertyChanged(nameof(OverviewTiles));
        OnPropertyChanged(nameof(MarketOverviewTiles));
        OnPropertyChanged(nameof(TrainingDeskItems));
        OnPropertyChanged(nameof(TrainingChecklist));
        OnPropertyChanged(nameof(RiskRules));
        OnPropertyChanged(nameof(SignalFacts));
        OnPropertyChanged(nameof(ScenarioFacts));
        OnPropertyChanged(nameof(QuickHelp));
        OnPropertyChanged(nameof(LeadSymbol));
        OnPropertyChanged(nameof(LeadHeadline));
        OnPropertyChanged(nameof(LeadScoreText));
        OnPropertyChanged(nameof(LeadLagText));
        OnPropertyChanged(nameof(LeadThemesText));
        OnPropertyChanged(nameof(MarketHeadline));
        OnPropertyChanged(nameof(SentimentSummary));
        OnPropertyChanged(nameof(WatchlistCaption));
        OnPropertyChanged(nameof(BestModelDetail));
        OnPropertyChanged(nameof(PricePreview));
        OnPropertyChanged(nameof(TrendPreview));
        OnPropertyChanged(nameof(ForecastPreview));
        OnPropertyChanged(nameof(IndicatorPreview));
    }

    private static IReadOnlyList<DashboardMetricTile> BuildOverviewTiles(DashboardSummary? summary)
    {
        if (summary?.LastRun is null)
        {
            return new[]
            {
                new DashboardMetricTile("Names tracked", "0", "Waiting for a run", "neutral"),
                new DashboardMetricTile("Ready now", "0", "No completed scan yet", "neutral"),
                new DashboardMetricTile("Data age", "n/a", "Run the engine first", "neutral"),
                new DashboardMetricTile("Model check", "n/a", "No review loaded", "neutral"),
            };
        }

        var lastRun = summary.LastRun;
        var passRate = lastRun.UniverseCount == 0
            ? 0.0
            : (double)lastRun.EligibleCount / lastRun.UniverseCount;
        var bestModel = SelectBestMetric(summary.BacktestMetrics);

        return new[]
        {
            new DashboardMetricTile("Names tracked", lastRun.UniverseCount.ToString("N0", CultureInfo.InvariantCulture), "Symbols checked", "neutral"),
            new DashboardMetricTile("Ready now", lastRun.EligibleCount.ToString("N0", CultureInfo.InvariantCulture), $"{passRate:P0} passed the checks", passRate >= 0.4 ? "positive" : "neutral"),
            new DashboardMetricTile("Data age", $"{lastRun.WorstLagDays}d", $"Typical age {lastRun.MedianLagDays:F1}d", lastRun.WorstLagDays <= 2 ? "positive" : "warn"),
            new DashboardMetricTile("Model check", ToFriendlyMetricName(bestModel?.Model), bestModel?.Accuracy is double accuracy ? $"Right about {accuracy:P0} of the time" : "No review loaded", "neutral"),
        };
    }

    private static IReadOnlyList<DashboardFact> BuildSignalFacts(DashboardSummary? summary)
    {
        var lead = summary?.TopSymbols.FirstOrDefault();
        var bestModel = SelectBestMetric(summary?.BacktestMetrics ?? Array.Empty<BacktestMetricRow>());
        if (lead is null)
        {
            return new[]
            {
                new DashboardFact("Market mood", "No scored symbols loaded yet."),
                new DashboardFact("Why it matters", "Scores help sort ideas, but they do not make the decision for you."),
                new DashboardFact("Data age", "Lower lag means newer data."),
            };
        }

        var signalBias = lead.Score >= 0.85 ? "Constructive" : lead.Score >= 0.7 ? "Measured" : "Selective";
        return new[]
        {
            new DashboardFact("Market mood", signalBias, lead.Score >= 0.85 ? "positive" : "neutral"),
            new DashboardFact("Main checks", FormatGateText(lead.GatesPassed)),
            new DashboardFact("Model view", ToFriendlyMetricName(bestModel?.Model)),
            new DashboardFact("Data age", FormatLagText(lead.LagDays), lead.LagDays <= 2 ? "positive" : "warn"),
        };
    }

    private static IReadOnlyList<DashboardFact> BuildScenarioFacts(DashboardSummary? summary)
    {
        if (summary?.CauseEffect is null)
        {
            return new[]
            {
                new DashboardFact("What-if desk", "Use the policy simulator to test one shock at a time."),
                new DashboardFact("Past cases", "Past event matches appear here after you build event links."),
                new DashboardFact("Confidence", "More linked history usually means a steadier estimate."),
            };
        }

        var causeEffect = summary.CauseEffect;
        var bestModel = SelectBestMetric(summary.BacktestMetrics);
        var confidence = causeEffect.EventImpactRows >= 100 && (bestModel?.Accuracy ?? 0) >= 0.58
            ? "Moderate"
            : "Measured";

        return new[]
        {
            new DashboardFact(
                "Latest event",
                $"{ToFriendlySentence(causeEffect.TopContextMetric, "linked event")} on {NormalizeText(causeEffect.TopContextDay, "the latest saved day")}"),
            new DashboardFact("Past cases", $"{causeEffect.EventImpactRows:N0} saved event outcomes"),
            new DashboardFact("What-if desk", causeEffect.EventImpactRows >= 100 ? "Ready for beginner what-if practice." : "Add more event history for steadier examples."),
            new DashboardFact("Confidence", confidence),
        };
    }

    private static string BuildHeaderSubtitle(DashboardSummary? summary)
    {
        if (summary?.LastRun is null)
        {
            return "Run the offline engine once to replace sample cards with your own results.";
        }

        var lastRun = summary.LastRun;
        return $"Latest run: {lastRun.RunId}. It found {lastRun.EligibleCount:N0} usable names, and the oldest data was {lastRun.WorstLagDays} day(s) behind.";
    }

    private static string BuildMarketHeadline(DashboardSummary? summary)
    {
        if (summary?.CauseEffect is null)
        {
            return "Event context is not loaded yet, so this view is using price-only signals.";
        }

        var causeEffect = summary.CauseEffect;
        return $"Watch item: {ToFriendlySentence(causeEffect.TopContextMetric, "linked event")} was most active on {NormalizeText(causeEffect.TopContextDay, "the latest saved day")}.";
    }

    private static string BuildSentimentSummary(DashboardSummary? summary)
    {
        if (summary?.LastRun is null)
        {
            return "Tone: Waiting for fresh inputs.";
        }

        var bestModel = SelectBestMetric(summary.BacktestMetrics);
        var worstLag = summary.LastRun.WorstLagDays;
        var accuracy = bestModel?.Accuracy ?? 0.0;
        var tone = worstLag <= 2 && accuracy >= 0.58
            ? "Constructive"
            : worstLag <= 4
                ? "Measured"
                : "Cautious";
        return $"Tone: {tone}. Start with the score, then check risk and timing before you act.";
    }

    private static string BuildWatchlistCaption(DashboardSummary? summary)
    {
        if (summary?.TopSymbols.Count > 0)
        {
            return "Sorted by fit score from the latest completed run.";
        }

        return "Top-ranked names appear here after a completed scan.";
    }

    private static IReadOnlyList<DashboardCallout> BuildQuickHelp()
    {
        return new[]
        {
            new DashboardCallout("Score", "Higher score usually means a stronger fit to the current checks."),
            new DashboardCallout("Data age", "This tells you how old the newest market data is."),
            new DashboardCallout("Forecast", "This is a practice estimate, not a promise."),
        };
    }

    private static string BuildBestModelDetail(DashboardSummary? summary)
    {
        var bestModel = SelectBestMetric(summary?.BacktestMetrics ?? Array.Empty<BacktestMetricRow>());
        if (bestModel is null)
        {
            return "Model check detail is not available yet.";
        }

        var accuracyText = bestModel.Accuracy is double accuracy
            ? accuracy.ToString("P0", CultureInfo.InvariantCulture)
            : "n/a";
        var f1Text = bestModel.F1 is double f1
            ? f1.ToString("P0", CultureInfo.InvariantCulture)
            : "n/a";
        return $"{ToFriendlyMetricName(bestModel.Model)} is the steadiest model in the current check. It was right about {accuracyText} of the time with balance score {f1Text}.";
    }

    private static (PriceSeriesModel Price, ForecastOverlayModel Forecast, IndicatorSeriesModel Indicator) BuildVisualModels(
        DashboardSummary? summary)
    {
        var priceModel = BuildDeterministicPreviewSeries(summary);
        var forecast = BuildForecast(priceModel, summary);
        var indicator = BuildIndicator(priceModel);
        return (priceModel, forecast, indicator);
    }

    private static PriceSeriesModel BuildDeterministicPreviewSeries(DashboardSummary? summary)
    {
        var pointCount = 42;
        var leadScore = summary?.TopSymbols.FirstOrDefault()?.Score ?? 0.82;
        var eligibleRatio = summary?.LastRun is null || summary.LastRun.UniverseCount == 0
            ? 0.45
            : (double)summary.LastRun.EligibleCount / summary.LastRun.UniverseCount;
        var basePrice = 118.0 + (leadScore * 24.0);
        var slope = 0.22 + (eligibleRatio * 0.55);
        var start = (summary?.LastRun?.FinishedAt ?? new DateTime(2026, 1, 31, 12, 0, 0, DateTimeKind.Utc)).Date.AddDays(-pointCount + 1);

        var timestamps = new List<DateTime>(pointCount);
        var opens = new List<double>(pointCount);
        var highs = new List<double>(pointCount);
        var lows = new List<double>(pointCount);
        var closes = new List<double>(pointCount);

        var priorClose = basePrice;
        for (var index = 0; index < pointCount; index++)
        {
            var wave = Math.Sin(index / 3.3) * 1.8 + Math.Cos(index / 4.9) * 1.15;
            var drift = slope * index;
            var close = basePrice + drift + wave;
            var open = index == 0 ? close - 0.6 : priorClose + Math.Sin(index * 0.58) * 0.45;
            var high = Math.Max(open, close) + 0.9 + ((index % 4) * 0.07);
            var low = Math.Min(open, close) - 0.85 - ((index % 3) * 0.06);

            timestamps.Add(start.AddDays(index));
            opens.Add(open);
            highs.Add(high);
            lows.Add(low);
            closes.Add(close);
            priorClose = close;
        }

        return new PriceSeriesModel(timestamps, closes, opens, highs, lows);
    }

    private static ForecastOverlayModel BuildForecast(PriceSeriesModel priceModel, DashboardSummary? summary)
    {
        var leadScore = summary?.TopSymbols.FirstOrDefault()?.Score ?? 0.8;
        var horizon = 6;
        var yhat = new List<double>(horizon);
        var lo = new List<double>(horizon);
        var hi = new List<double>(horizon);
        var timestamps = new List<DateTime>(horizon);
        var lastClose = priceModel.Values.LastOrDefault();
        var lastTimestamp = priceModel.Timestamps.LastOrDefault();

        for (var index = 0; index < horizon; index++)
        {
            var center = lastClose + ((index + 1) * (0.38 + leadScore * 0.22)) + Math.Sin((index + 2) / 2.5) * 0.25;
            var band = 0.95 + (index * 0.18);
            yhat.Add(center);
            lo.Add(center - band);
            hi.Add(center + band);
            timestamps.Add(lastTimestamp == default ? DateTime.UtcNow.Date.AddDays(index + 1) : lastTimestamp.AddDays(index + 1));
        }

        return new ForecastOverlayModel(
            TrainedUntil: lastTimestamp == default ? DateTime.UtcNow.Date : lastTimestamp,
            HorizonPoints: horizon,
            YHat: yhat,
            Lo: lo,
            Hi: hi,
            ForecastTimestamps: timestamps);
    }

    private static IndicatorSeriesModel BuildIndicator(PriceSeriesModel priceModel)
    {
        if (priceModel.Values.Count == 0)
        {
            return EmptyIndicatorSeries();
        }

        var values = new List<double>(priceModel.Values.Count);
        values.Add(50.0);
        for (var index = 1; index < priceModel.Values.Count; index++)
        {
            var delta = priceModel.Values[index] - priceModel.Values[index - 1];
            var normalized = 50.0 + (Math.Tanh(delta) * 24.0);
            values.Add(Math.Clamp(normalized, 5.0, 95.0));
        }

        return new IndicatorSeriesModel(priceModel.Timestamps, values);
    }

    private static BacktestMetricRow? SelectBestMetric(IReadOnlyList<BacktestMetricRow> metrics)
    {
        return metrics
            .OrderByDescending(metric => metric.Accuracy ?? double.MinValue)
            .ThenBy(metric => metric.Mse ?? double.MaxValue)
            .FirstOrDefault();
    }

    private static string FormatFriendlyList(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value
            .Replace(';', ',')
            .Replace('|', ',')
            .Trim();
    }

    private static string FormatGateText(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Check result not available";
        }

        return value.Equals("yes", StringComparison.OrdinalIgnoreCase)
            ? "Passed the main checks"
            : value.Equals("no", StringComparison.OrdinalIgnoreCase)
                ? "Did not pass every main check"
                : value.Trim();
    }

    private static string FormatLagText(int lagDays)
    {
        return lagDays == 1
            ? "1 day old"
            : $"{lagDays} days old";
    }

    private static string ToFriendlyMetricName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "No model selected";
        }

        return value
            .Replace('_', ' ')
            .Trim();
    }

    private static string ToFriendlySentence(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var text = value.Replace('_', ' ').Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return fallback;
        }

        return char.ToUpperInvariant(text[0]) + text[1..];
    }

    private static string NormalizeText(string? value, string fallback)
    {
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }

    private static PriceSeriesModel EmptyPriceSeries()
    {
        return new PriceSeriesModel(Array.Empty<DateTime>(), Array.Empty<double>());
    }

    private static IndicatorSeriesModel EmptyIndicatorSeries()
    {
        return new IndicatorSeriesModel(Array.Empty<DateTime>(), Array.Empty<double>());
    }
}
