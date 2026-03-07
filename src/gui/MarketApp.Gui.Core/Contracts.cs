using Microsoft.Maui.Controls;

namespace MarketApp.Gui.Core;

public sealed record PriceSeriesModel(
    IReadOnlyList<DateTime> Timestamps,
    IReadOnlyList<double> Values,
    IReadOnlyList<double>? Opens = null,
    IReadOnlyList<double>? Highs = null,
    IReadOnlyList<double>? Lows = null
);

public sealed record IndicatorSeriesModel(IReadOnlyList<DateTime> Timestamps, IReadOnlyList<double> Values);

public sealed record ForecastOverlayModel(
    DateTime TrainedUntil,
    int HorizonPoints,
    IReadOnlyList<double> YHat,
    IReadOnlyList<double> Lo,
    IReadOnlyList<double> Hi,
    IReadOnlyList<DateTime>? ForecastTimestamps = null
);

public interface IChartProvider
{
    View CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast);
    View CreateIndicatorChart(IndicatorSeriesModel model);
}

public interface ISecretsStore
{
    Task SetAsync(string key, string value);
    Task<string?> GetAsync(string key);
    Task RemoveAsync(string key);
}
