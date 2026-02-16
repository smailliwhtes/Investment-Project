namespace MarketApp.Gui.Core;

public sealed record PriceSeriesModel(IReadOnlyList<DateTime> Timestamps, IReadOnlyList<double> Values);
public sealed record IndicatorSeriesModel(IReadOnlyList<DateTime> Timestamps, IReadOnlyList<double> Values);
public sealed record ForecastOverlayModel(
    DateTime TrainedUntil,
    int HorizonPoints,
    IReadOnlyList<double> YHat,
    IReadOnlyList<double> Lo,
    IReadOnlyList<double> Hi
);

public interface IChartProvider
{
    object CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast);
    object CreateIndicatorChart(IndicatorSeriesModel model);
}

public interface ISecretsStore
{
    Task SetAsync(string key, string value);
    Task<string?> GetAsync(string key);
    Task RemoveAsync(string key);
}
