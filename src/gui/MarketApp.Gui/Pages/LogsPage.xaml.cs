using MarketApp.Gui.Core;

namespace MarketApp.Gui.Pages;

public partial class LogsPage : ContentPage
{
    public LogsPage(LogsViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = viewModel;
    }
}
