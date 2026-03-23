using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class ParquetConverterPage : ContentPage
{
    public ParquetConverterPage(ParquetConverterViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
