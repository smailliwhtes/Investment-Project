using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class UniversePage : ContentPage
{
    public UniversePage(UniverseViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
