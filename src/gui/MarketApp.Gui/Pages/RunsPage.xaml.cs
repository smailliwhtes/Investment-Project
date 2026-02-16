using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class RunsPage : ContentPage
{
    public RunsPage(RunsViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
