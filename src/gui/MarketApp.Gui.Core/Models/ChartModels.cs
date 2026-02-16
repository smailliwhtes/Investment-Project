namespace MarketApp.Gui.Core.Models;

public sealed record PriceSeriesModel(DateTime[] Ts, double[] Open, double[] High, double[] Low, double[] Close, double[]? Volume);

public sealed record ForecastOverlayModel(DateTime[] TsF, double[] YHat, double[]? Lo, double[]? Hi, string ModelName, DateTime TrainedUntil);

public sealed record IndicatorSeriesModel(string Name, DateTime[] Ts, double[] Values);
