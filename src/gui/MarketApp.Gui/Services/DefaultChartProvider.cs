using MarketApp.Gui.Core;
using Microsoft.Maui.Graphics;

namespace MarketApp.Gui.Services;

public class DefaultChartProvider : IChartProvider
{
    private const int MaxPlotPoints = 2500;

    public View CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast)
    {
        var close = Decimate(model.Values, MaxPlotPoints);
        if (close.Count == 0)
        {
            return new Label { Text = "No price data available." };
        }

        var opens = model.Opens is { Count: > 0 } ? Decimate(model.Opens, MaxPlotPoints) : BuildSyntheticOpen(close);
        var highs = model.Highs is { Count: > 0 } ? Decimate(model.Highs, MaxPlotPoints) : BuildSyntheticHigh(opens, close);
        var lows = model.Lows is { Count: > 0 } ? Decimate(model.Lows, MaxPlotPoints) : BuildSyntheticLow(opens, close);

        var yHat = forecast?.YHat ?? Array.Empty<double>();
        var lo = forecast?.Lo ?? Array.Empty<double>();
        var hi = forecast?.Hi ?? Array.Empty<double>();

        var drawable = new PriceChartDrawable(opens, highs, lows, close, yHat, lo, hi);
        var chartWidth = Math.Max(700, close.Count * 4);

        var graphics = new GraphicsView
        {
            Drawable = drawable,
            HeightRequest = 230,
            WidthRequest = chartWidth,
            BackgroundColor = Colors.Transparent,
        };

        var scroll = new ScrollView
        {
            Orientation = ScrollOrientation.Horizontal,
            HeightRequest = 240,
            Content = graphics,
        };

        var trainedUntilText = forecast is null
            ? "Forecast overlay: none"
            : $"Forecast trained-until: {forecast.TrainedUntil:yyyy-MM-dd} | horizon: {forecast.HorizonPoints}";

        return new VerticalStackLayout
        {
            Spacing = 4,
            Children =
            {
                new Label { Text = "Signal Trend + Forecast", FontAttributes = FontAttributes.Bold, TextColor = Color.FromArgb("#102235") },
                new Label { Text = trainedUntilText, FontSize = 12, TextColor = Color.FromArgb("#5C6F86") },
                scroll,
                new Label { Text = "Preview only. Use it to compare paths, not to assume certainty.", FontSize = 11, TextColor = Color.FromArgb("#5C6F86") },
            },
        };
    }

    public View CreateIndicatorChart(IndicatorSeriesModel model)
    {
        var values = Decimate(model.Values, MaxPlotPoints);
        if (values.Count == 0)
        {
            return new Label { Text = "No indicator data available." };
        }

        var drawable = new IndicatorChartDrawable(values);
        var chartWidth = Math.Max(700, values.Count * 3);

        var graphics = new GraphicsView
        {
            Drawable = drawable,
            HeightRequest = 160,
            WidthRequest = chartWidth,
            BackgroundColor = Colors.Transparent,
        };

        return new VerticalStackLayout
        {
            Spacing = 4,
            Children =
            {
                new Label { Text = "Momentum Preview", FontAttributes = FontAttributes.Bold, TextColor = Color.FromArgb("#102235") },
                new ScrollView
                {
                    Orientation = ScrollOrientation.Horizontal,
                    HeightRequest = 170,
                    Content = graphics,
                },
            },
        };
    }

    private static IReadOnlyList<double> Decimate(IReadOnlyList<double> source, int maxPoints)
    {
        if (source.Count <= maxPoints)
        {
            return source;
        }

        var output = new double[maxPoints];
        var step = (source.Count - 1d) / (maxPoints - 1d);
        for (var i = 0; i < maxPoints; i++)
        {
            var idx = (int)Math.Round(i * step, MidpointRounding.AwayFromZero);
            idx = Math.Clamp(idx, 0, source.Count - 1);
            output[i] = source[idx];
        }

        return output;
    }

    private static IReadOnlyList<double> BuildSyntheticOpen(IReadOnlyList<double> close)
    {
        var result = new double[close.Count];
        for (var i = 0; i < close.Count; i++)
        {
            result[i] = i == 0 ? close[i] : close[i - 1];
        }

        return result;
    }

    private static IReadOnlyList<double> BuildSyntheticHigh(IReadOnlyList<double> open, IReadOnlyList<double> close)
    {
        var result = new double[close.Count];
        for (var i = 0; i < close.Count; i++)
        {
            result[i] = Math.Max(open[i], close[i]) * 1.01;
        }

        return result;
    }

    private static IReadOnlyList<double> BuildSyntheticLow(IReadOnlyList<double> open, IReadOnlyList<double> close)
    {
        var result = new double[close.Count];
        for (var i = 0; i < close.Count; i++)
        {
            result[i] = Math.Min(open[i], close[i]) * 0.99;
        }

        return result;
    }

    private sealed class PriceChartDrawable : IDrawable
    {
        private readonly IReadOnlyList<double> _open;
        private readonly IReadOnlyList<double> _high;
        private readonly IReadOnlyList<double> _low;
        private readonly IReadOnlyList<double> _close;
        private readonly IReadOnlyList<double> _forecast;
        private readonly IReadOnlyList<double> _forecastLo;
        private readonly IReadOnlyList<double> _forecastHi;

        public PriceChartDrawable(
            IReadOnlyList<double> open,
            IReadOnlyList<double> high,
            IReadOnlyList<double> low,
            IReadOnlyList<double> close,
            IReadOnlyList<double> forecast,
            IReadOnlyList<double> forecastLo,
            IReadOnlyList<double> forecastHi)
        {
            _open = open;
            _high = high;
            _low = low;
            _close = close;
            _forecast = forecast;
            _forecastLo = forecastLo;
            _forecastHi = forecastHi;
        }

        public void Draw(ICanvas canvas, RectF dirtyRect)
        {
            if (_close.Count < 2)
            {
                return;
            }

            var allValues = new List<double>(_high.Count + _low.Count + _forecast.Count + _forecastLo.Count + _forecastHi.Count);
            allValues.AddRange(_high);
            allValues.AddRange(_low);
            allValues.AddRange(_forecast);
            allValues.AddRange(_forecastLo);
            allValues.AddRange(_forecastHi);

            var min = allValues.Min();
            var max = allValues.Max();
            if (Math.Abs(max - min) < 1e-9)
            {
                max = min + 1;
            }

            var plotLeft = 10f;
            var plotRight = dirtyRect.Width - 10f;
            var plotTop = 10f;
            var plotBottom = dirtyRect.Height - 10f;
            var width = Math.Max(plotRight - plotLeft, 1f);
            var height = Math.Max(plotBottom - plotTop, 1f);
            var candleWidth = Math.Max(1.5f, width / Math.Max(_close.Count * 1.8f, 1f));

            float Y(double value) => (float)(plotBottom - ((value - min) / (max - min)) * height);
            float X(int idx, int count) => plotLeft + (float)idx / Math.Max(1, count - 1) * width;

            canvas.FillColor = Color.FromArgb("#F4F7FB");
            canvas.FillRectangle(plotLeft, plotTop, width, height);

            canvas.StrokeColor = Color.FromArgb("#D6E0EB");
            canvas.StrokeSize = 1;
            for (var i = 1; i < 5; i++)
            {
                var y = plotTop + (height / 5f) * i;
                canvas.DrawLine(plotLeft, y, plotRight, y);
            }

            for (var i = 0; i < _close.Count; i++)
            {
                var x = X(i, _close.Count);
                var yOpen = Y(_open[i]);
                var yClose = Y(_close[i]);
                var yHigh = Y(_high[i]);
                var yLow = Y(_low[i]);

                canvas.StrokeColor = Colors.Gray;
                canvas.StrokeSize = 1;
                canvas.DrawLine(x, yHigh, x, yLow);

                var top = Math.Min(yOpen, yClose);
                var bodyHeight = Math.Max(1f, Math.Abs(yOpen - yClose));
                canvas.FillColor = _close[i] >= _open[i]
                    ? Color.FromArgb("#1C8A57")
                    : Color.FromArgb("#C64545");
                canvas.FillRectangle(x - (candleWidth / 2), top, candleWidth, bodyHeight);
            }

            if (_forecast.Count > 0)
            {
                var startX = X(_close.Count - 1, _close.Count);
                var step = width / Math.Max(1, _close.Count - 1);

                if (_forecastLo.Count == _forecast.Count && _forecastHi.Count == _forecast.Count)
                {
                    canvas.StrokeColor = Colors.Transparent;
                    canvas.FillColor = Color.FromRgba(93, 127, 168, 56);
                    var band = new PathF();
                    band.MoveTo(startX, Y(_forecastHi[0]));
                    for (var i = 1; i < _forecastHi.Count; i++)
                    {
                        band.LineTo(startX + (float)(i * step), Y(_forecastHi[i]));
                    }
                    for (var i = _forecastLo.Count - 1; i >= 0; i--)
                    {
                        band.LineTo(startX + (float)(i * step), Y(_forecastLo[i]));
                    }
                    band.Close();
                    canvas.FillPath(band);
                }

                canvas.StrokeColor = Color.FromArgb("#315C88");
                canvas.StrokeSize = 1.5f;
                for (var i = 1; i < _forecast.Count; i++)
                {
                    var x0 = startX + (float)((i - 1) * step);
                    var x1 = startX + (float)(i * step);
                    canvas.DrawLine(x0, Y(_forecast[i - 1]), x1, Y(_forecast[i]));
                }
            }
        }
    }

    private sealed class IndicatorChartDrawable : IDrawable
    {
        private readonly IReadOnlyList<double> _values;

        public IndicatorChartDrawable(IReadOnlyList<double> values)
        {
            _values = values;
        }

        public void Draw(ICanvas canvas, RectF dirtyRect)
        {
            if (_values.Count < 2)
            {
                return;
            }

            var min = _values.Min();
            var max = _values.Max();
            if (Math.Abs(max - min) < 1e-9)
            {
                max = min + 1;
            }

            var plotLeft = 10f;
            var plotRight = dirtyRect.Width - 10f;
            var plotTop = 10f;
            var plotBottom = dirtyRect.Height - 10f;
            var width = Math.Max(plotRight - plotLeft, 1f);
            var height = Math.Max(plotBottom - plotTop, 1f);

            float Y(double value) => (float)(plotBottom - ((value - min) / (max - min)) * height);
            float X(int idx) => plotLeft + (float)idx / Math.Max(1, _values.Count - 1) * width;

            canvas.FillColor = Color.FromArgb("#F4F7FB");
            canvas.FillRectangle(plotLeft, plotTop, width, height);

            canvas.StrokeColor = Color.FromArgb("#D6E0EB");
            canvas.StrokeSize = 1;
            for (var i = 1; i < 4; i++)
            {
                var y = plotTop + (height / 4f) * i;
                canvas.DrawLine(plotLeft, y, plotRight, y);
            }

            canvas.StrokeColor = Color.FromArgb("#315C88");
            canvas.StrokeSize = 1.5f;
            for (var i = 1; i < _values.Count; i++)
            {
                canvas.DrawLine(X(i - 1), Y(_values[i - 1]), X(i), Y(_values[i]));
            }
        }
    }
}

