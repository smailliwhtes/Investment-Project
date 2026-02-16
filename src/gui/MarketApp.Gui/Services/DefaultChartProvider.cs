using MarketApp.Gui.Core;

namespace MarketApp.Gui.Services;

public class DefaultChartProvider : IChartProvider
{
    public object CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast)
    {
        var layout = new VerticalStackLayout { Spacing = 4, Padding = 8 };
        layout.Add(new Label { Text = "Price series (preview)", FontAttributes = FontAttributes.Bold });
        layout.Add(new Label { Text = $"Points: {model.Timestamps.Count}" });
        if (forecast is not null)
        {
            layout.Add(new Label { Text = $"Forecast horizon: {forecast.HorizonPoints}" });
        }

        layout.Add(new BoxView
        {
            HeightRequest = 120,
            BackgroundColor = Colors.LightGray,
            CornerRadius = 6
        });

        return layout;
    }

    public object CreateIndicatorChart(IndicatorSeriesModel model)
    {
        var layout = new VerticalStackLayout { Spacing = 4, Padding = 8 };
        layout.Add(new Label { Text = "Indicator series (preview)", FontAttributes = FontAttributes.Bold });
        layout.Add(new Label { Text = $"Points: {model.Timestamps.Count}" });
        layout.Add(new BoxView
        {
            HeightRequest = 120,
            BackgroundColor = Colors.LightGray,
            CornerRadius = 6
        });

        return layout;
    }
}
