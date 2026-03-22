using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class PolicySimulatorPage : ContentPage
{
    public PolicySimulatorPage(PolicySimulatorViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
