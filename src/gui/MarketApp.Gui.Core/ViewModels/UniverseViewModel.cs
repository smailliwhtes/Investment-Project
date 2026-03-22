using System.Globalization;

namespace MarketApp.Gui.Core;

public class UniverseViewModel : ViewModelBase
{
    private readonly IChartProvider _chartProvider;
    private readonly IReadOnlyList<RunSummary> _runHistory;
    private string _filterText = string.Empty;
    private ScoreRow? _selectedScore;
    private string _selectedDetailTab = "Overview";
    private IReadOnlyList<ScoreRow> _allScores = Array.Empty<ScoreRow>();
    private IReadOnlyList<ScoreRow> _filtered = Array.Empty<ScoreRow>();
    private Microsoft.Maui.Controls.View? _priceChart;
    private Microsoft.Maui.Controls.View? _indicatorChart;
    private string _explainSummary = "Pick a symbol to see the short reason it ranked here.";
    private string _qualitySummary = "Pick a symbol to see a short data-health note.";

    public UniverseViewModel(SampleDataService dataService, IChartProvider chartProvider)
    {
        _chartProvider = chartProvider;
        _runHistory = dataService.GetRunHistory();

        Title = "Universe & Scores";
        _allScores = dataService.GetScores();
        _filtered = _allScores;
        SelectedScore = _filtered.FirstOrDefault();
        UpdateSelectionDetails();
    }

    public IReadOnlyList<ScoreRow> FilteredScores
    {
        get => _filtered;
        private set => SetProperty(ref _filtered, value);
    }

    public string FilterText
    {
        get => _filterText;
        set
        {
            if (SetProperty(ref _filterText, value))
            {
                ApplyFilter();
            }
        }
    }

    public IReadOnlyList<string> DetailTabs { get; } = new[] { "Overview", "Why it ranked", "Data health" };

    public string SelectedDetailTab
    {
        get => _selectedDetailTab;
        set
        {
            if (SetProperty(ref _selectedDetailTab, value))
            {
                OnPropertyChanged(nameof(IsOverviewTab));
                OnPropertyChanged(nameof(IsExplainTab));
                OnPropertyChanged(nameof(IsQualityTab));
            }
        }
    }

    public bool IsOverviewTab => string.Equals(SelectedDetailTab, "Overview", StringComparison.OrdinalIgnoreCase);

    public bool IsExplainTab => string.Equals(SelectedDetailTab, "Why it ranked", StringComparison.OrdinalIgnoreCase);

    public bool IsQualityTab => string.Equals(SelectedDetailTab, "Data health", StringComparison.OrdinalIgnoreCase);

    public ScoreRow? SelectedScore
    {
        get => _selectedScore;
        set
        {
            if (SetProperty(ref _selectedScore, value))
            {
                UpdateSelectionDetails();
            }
        }
    }

    public Microsoft.Maui.Controls.View? PriceChart
    {
        get => _priceChart;
        private set => SetProperty(ref _priceChart, value);
    }

    public Microsoft.Maui.Controls.View? IndicatorChart
    {
        get => _indicatorChart;
        private set => SetProperty(ref _indicatorChart, value);
    }

    public string ExplainSummary
    {
        get => _explainSummary;
        private set => SetProperty(ref _explainSummary, value);
    }

    public string QualitySummary
    {
        get => _qualitySummary;
        private set => SetProperty(ref _qualitySummary, value);
    }

    private void ApplyFilter()
    {
        if (string.IsNullOrWhiteSpace(_filterText))
        {
            FilteredScores = _allScores;
            if (SelectedScore is null)
            {
                SelectedScore = FilteredScores.FirstOrDefault();
            }
            return;
        }

        FilteredScores = _allScores
            .Where(s => s.Symbol.Contains(_filterText, StringComparison.OrdinalIgnoreCase) ||
                        s.ThemeLabels.Contains(_filterText, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (SelectedScore is null || !FilteredScores.Any(s => string.Equals(s.Symbol, SelectedScore.Symbol, StringComparison.OrdinalIgnoreCase)))
        {
            SelectedScore = FilteredScores.FirstOrDefault();
        }
    }

    private void UpdateSelectionDetails()
    {
        if (SelectedScore is null)
        {
            ExplainSummary = "No symbol selected.";
            QualitySummary = "No symbol selected.";
            PriceChart = null;
            IndicatorChart = null;
            return;
        }

        var checksText = SelectedScore.GatesPassed.Equals("yes", StringComparison.OrdinalIgnoreCase)
            ? "It passed the main checks"
            : "It did not pass every main check";
        var flagText = SelectedScore.FlagsCount == 1
            ? "1 caution flag"
            : $"{SelectedScore.FlagsCount} caution flags";
        var themeText = string.IsNullOrWhiteSpace(SelectedScore.ThemeLabels)
            ? "no theme tags"
            : SelectedScore.ThemeLabels.Replace(';', ',').Replace('|', ',');
        var freshnessText = SelectedScore.LagDays <= 1
            ? "very fresh"
            : SelectedScore.LagDays <= 3
                ? "fairly fresh"
                : "older than ideal";

        ExplainSummary =
            $"{SelectedScore.Symbol} is ranked #{SelectedScore.Rank} with fit score {SelectedScore.Score:F2}. " +
            $"{checksText}, shows {flagText}, and is mostly linked to {themeText}.";

        QualitySummary =
            $"Newest saved price: {SelectedScore.LastDate}. This symbol is {SelectedScore.LagDays} day(s) behind the run date, so the data is {freshnessText}. " +
            "Lower lag is better because it means the view is using newer prices.";

        if (!TryBuildChartsFromOhlcv(SelectedScore.Symbol, out var priceModel, out var indicatorModel, out var forecast))
        {
            BuildDeterministicPreviewSeries(SelectedScore, out priceModel, out indicatorModel, out forecast);
        }

        PriceChart = _chartProvider.CreatePriceChart(priceModel, forecast);
        IndicatorChart = _chartProvider.CreateIndicatorChart(indicatorModel);
    }

    private bool TryBuildChartsFromOhlcv(
        string symbol,
        out PriceSeriesModel priceModel,
        out IndicatorSeriesModel indicatorModel,
        out ForecastOverlayModel forecast)
    {
        priceModel = default!;
        indicatorModel = default!;
        forecast = default!;

        var ohlcvDir = ResolveOhlcvDirectory();
        if (string.IsNullOrWhiteSpace(ohlcvDir))
        {
            return false;
        }

        var filePath = Path.Combine(ohlcvDir, $"{symbol.ToUpperInvariant()}.csv");
        if (!File.Exists(filePath))
        {
            return false;
        }

        if (!TryReadOhlcvRows(filePath, out var timestamps, out var opens, out var highs, out var lows, out var closes))
        {
            return false;
        }

        var take = Math.Min(240, closes.Count);
        if (take < 40)
        {
            return false;
        }

        var ts = timestamps.Skip(timestamps.Count - take).ToArray();
        var o = opens.Skip(opens.Count - take).ToArray();
        var h = highs.Skip(highs.Count - take).ToArray();
        var l = lows.Skip(lows.Count - take).ToArray();
        var c = closes.Skip(closes.Count - take).ToArray();

        var indicator = BuildRsi(c, 14);
        var horizon = 20;
        var yhat = new double[horizon];
        var lo = new double[horizon];
        var hi = new double[horizon];
        var fTs = new DateTime[horizon];

        var drift = (c[^1] - c[Math.Max(0, c.Length - 21)]) / 20.0;
        var volatility = StandardDeviation(c.Skip(Math.Max(0, c.Length - 40)).ToArray());
        if (double.IsNaN(volatility) || volatility <= 0)
        {
            volatility = Math.Max(0.5, c[^1] * 0.01);
        }

        for (var i = 0; i < horizon; i++)
        {
            var value = c[^1] + ((i + 1) * drift);
            yhat[i] = value;
            lo[i] = value - (1.28 * volatility);
            hi[i] = value + (1.28 * volatility);
            fTs[i] = ts[^1].AddDays(i + 1);
        }

        priceModel = new PriceSeriesModel(ts, c, o, h, l);
        indicatorModel = new IndicatorSeriesModel(ts, indicator);
        forecast = new ForecastOverlayModel(
            TrainedUntil: ts[^1],
            HorizonPoints: horizon,
            YHat: yhat,
            Lo: lo,
            Hi: hi,
            ForecastTimestamps: fTs);
        return true;
    }

    private string? ResolveOhlcvDirectory()
    {
        var fromEnv = Environment.GetEnvironmentVariable("MARKETAPP_OHLCV_DIR");
        if (!string.IsNullOrWhiteSpace(fromEnv) && Directory.Exists(fromEnv))
        {
            return fromEnv;
        }

        var latestRun = _runHistory
            .Where(r => !string.IsNullOrWhiteSpace(r.RunDirectory))
            .OrderByDescending(r => r.FinishedAt)
            .FirstOrDefault();

        if (latestRun is null)
        {
            return null;
        }

        var snapshotPath = Path.Combine(latestRun.RunDirectory, "config_snapshot.yaml");
        if (!File.Exists(snapshotPath))
        {
            return null;
        }

        string? rawPath = null;
        foreach (var line in File.ReadLines(snapshotPath))
        {
            var trimmed = line.Trim();
            if (!trimmed.StartsWith("ohlcv_daily_dir:", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            rawPath = trimmed["ohlcv_daily_dir:".Length..].Trim().Trim('"', '\'');
            break;
        }

        if (string.IsNullOrWhiteSpace(rawPath))
        {
            return null;
        }

        if (Path.IsPathRooted(rawPath) && Directory.Exists(rawPath))
        {
            return rawPath;
        }

        var candidates = new[]
        {
            Path.GetFullPath(Path.Combine(latestRun.RunDirectory, rawPath)),
            Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), rawPath)),
            Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "market_app", rawPath)),
        };

        return candidates.FirstOrDefault(Directory.Exists);
    }

    private static bool TryReadOhlcvRows(
        string filePath,
        out List<DateTime> timestamps,
        out List<double> opens,
        out List<double> highs,
        out List<double> lows,
        out List<double> closes)
    {
        timestamps = new List<DateTime>();
        opens = new List<double>();
        highs = new List<double>();
        lows = new List<double>();
        closes = new List<double>();

        var lines = File.ReadAllLines(filePath);
        if (lines.Length < 2)
        {
            return false;
        }

        var headers = lines[0].Split(',').Select(h => h.Trim().ToLowerInvariant()).ToArray();
        var idxDate = Array.IndexOf(headers, "date");
        var idxOpen = Array.IndexOf(headers, "open");
        var idxHigh = Array.IndexOf(headers, "high");
        var idxLow = Array.IndexOf(headers, "low");
        var idxClose = Array.IndexOf(headers, "close");
        if (idxDate < 0 || idxOpen < 0 || idxHigh < 0 || idxLow < 0 || idxClose < 0)
        {
            return false;
        }

        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var parts = line.Split(',');
            if (parts.Length <= Math.Max(idxClose, Math.Max(idxHigh, idxLow)))
            {
                continue;
            }

            if (!DateTime.TryParse(parts[idxDate], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var date))
            {
                continue;
            }
            if (!double.TryParse(parts[idxOpen], NumberStyles.Float, CultureInfo.InvariantCulture, out var open))
            {
                continue;
            }
            if (!double.TryParse(parts[idxHigh], NumberStyles.Float, CultureInfo.InvariantCulture, out var high))
            {
                continue;
            }
            if (!double.TryParse(parts[idxLow], NumberStyles.Float, CultureInfo.InvariantCulture, out var low))
            {
                continue;
            }
            if (!double.TryParse(parts[idxClose], NumberStyles.Float, CultureInfo.InvariantCulture, out var close))
            {
                continue;
            }

            timestamps.Add(DateTime.SpecifyKind(date.Date, DateTimeKind.Utc));
            opens.Add(open);
            highs.Add(high);
            lows.Add(low);
            closes.Add(close);
        }

        return closes.Count > 1;
    }

    private static double[] BuildRsi(IReadOnlyList<double> close, int period)
    {
        var rsi = Enumerable.Repeat(50.0, close.Count).ToArray();
        if (close.Count <= period)
        {
            return rsi;
        }

        double avgGain = 0;
        double avgLoss = 0;
        for (var i = 1; i <= period; i++)
        {
            var delta = close[i] - close[i - 1];
            if (delta >= 0)
            {
                avgGain += delta;
            }
            else
            {
                avgLoss += -delta;
            }
        }

        avgGain /= period;
        avgLoss /= period;
        rsi[period] = avgLoss == 0 ? 100.0 : 100.0 - (100.0 / (1 + (avgGain / avgLoss)));

        for (var i = period + 1; i < close.Count; i++)
        {
            var delta = close[i] - close[i - 1];
            var gain = Math.Max(delta, 0);
            var loss = Math.Max(-delta, 0);
            avgGain = ((avgGain * (period - 1)) + gain) / period;
            avgLoss = ((avgLoss * (period - 1)) + loss) / period;
            rsi[i] = avgLoss == 0 ? 100.0 : 100.0 - (100.0 / (1 + (avgGain / avgLoss)));
        }

        return rsi;
    }

    private static double StandardDeviation(IReadOnlyList<double> values)
    {
        if (values.Count < 2)
        {
            return double.NaN;
        }

        var mean = values.Average();
        var variance = values.Select(v => (v - mean) * (v - mean)).Average();
        return Math.Sqrt(variance);
    }

    private static void BuildDeterministicPreviewSeries(
        ScoreRow selected,
        out PriceSeriesModel priceModel,
        out IndicatorSeriesModel indicatorModel,
        out ForecastOverlayModel forecast)
    {
        var seed = Math.Abs(selected.Symbol.GetHashCode(StringComparison.Ordinal));
        var points = 240;
        var timestamps = new List<DateTime>(points);
        var opens = new List<double>(points);
        var highs = new List<double>(points);
        var lows = new List<double>(points);
        var closes = new List<double>(points);

        var start = DateTime.UtcNow.Date.AddDays(-points + 1);
        var baseLevel = Math.Max(15.0, 35.0 + (selected.Score * 12.0));
        for (var i = 0; i < points; i++)
        {
            var day = start.AddDays(i);
            var drift = i * (0.01 + (selected.Score * 0.001));
            var wave = Math.Sin((i + (seed % 23)) * 0.22) * 1.25;
            var close = baseLevel + drift + wave;
            var open = i == 0 ? close : closes[i - 1];
            var high = Math.Max(open, close) * 1.006;
            var low = Math.Min(open, close) * 0.994;

            timestamps.Add(day);
            opens.Add(open);
            highs.Add(high);
            lows.Add(low);
            closes.Add(close);
        }

        var horizon = 20;
        var yhat = new List<double>(horizon);
        var lo = new List<double>(horizon);
        var hi = new List<double>(horizon);
        var forecastTimestamps = new List<DateTime>(horizon);
        var last = closes[^1];
        for (var i = 0; i < horizon; i++)
        {
            var drift = (i + 1) * 0.08;
            var seasonal = Math.Sin((i + (seed % 11)) * 0.45) * 0.3;
            var value = last + drift + seasonal;
            yhat.Add(value);
            lo.Add(value - 0.9);
            hi.Add(value + 0.9);
            forecastTimestamps.Add(timestamps[^1].AddDays(i + 1));
        }

        var indicatorValues = closes
            .Select((value, idx) => 50.0 + (Math.Sin((idx + (seed % 19)) * 0.15) * 20.0))
            .ToArray();

        priceModel = new PriceSeriesModel(timestamps, closes, opens, highs, lows);
        indicatorModel = new IndicatorSeriesModel(timestamps, indicatorValues);
        forecast = new ForecastOverlayModel(
            TrainedUntil: timestamps[^1],
            HorizonPoints: horizon,
            YHat: yhat,
            Lo: lo,
            Hi: hi,
            ForecastTimestamps: forecastTimestamps);
    }
}
