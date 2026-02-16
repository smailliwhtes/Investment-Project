namespace MarketApp.Gui.Core.Abstractions;

public interface IChartProvider
{
    object CreatePriceChart(Models.PriceSeriesModel model, Models.ForecastOverlayModel? forecast);
    object CreateIndicatorChart(Models.IndicatorSeriesModel model);
}
