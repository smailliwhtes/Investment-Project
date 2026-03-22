using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class DashboardPage : ContentPage
{
    private readonly IChartProvider _chartProvider;
    private readonly DashboardViewModel _viewModel;

    public DashboardPage(DashboardViewModel viewModel, IChartProvider chartProvider)
    {
        InitializeComponent();
        _viewModel = viewModel;
        _chartProvider = chartProvider;
        BindingContext = viewModel;
        _viewModel.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(DashboardViewModel.TrendPreview)
                or nameof(DashboardViewModel.ForecastPreview)
                or nameof(DashboardViewModel.IndicatorPreview)
                or nameof(DashboardViewModel.Summary))
            {
                RenderCharts();
            }
        };
    }

    protected override void OnAppearing()
    {
        base.OnAppearing();
        RenderCharts();
    }

    private void RenderCharts()
    {
        TrendChartHost.Content = _chartProvider.CreatePriceChart(
            _viewModel.TrendPreview,
            _viewModel.ForecastPreview);
        IndicatorChartHost.Content = _chartProvider.CreateIndicatorChart(_viewModel.IndicatorPreview);
    }
}
