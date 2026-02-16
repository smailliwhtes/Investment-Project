using MarketApp.Gui.Core.Abstractions;
using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Controls;

public sealed class LiveChartsChartProvider : IChartProvider
{
    public object CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast)
    {
        return new Label { Text = $"LiveCharts2 price chart placeholder ({model.Ts.Length} points)" };
    }

    public object CreateIndicatorChart(IndicatorSeriesModel model)
    {
        return new Label { Text = $"Indicator chart placeholder: {model.Name}" };
    }
}
