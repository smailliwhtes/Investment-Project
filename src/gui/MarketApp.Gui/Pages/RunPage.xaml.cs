using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class RunPage : ContentPage
{
    public RunPage(RunViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
