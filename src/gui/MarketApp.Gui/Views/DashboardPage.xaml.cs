using MarketApp.Gui.Core.ViewModels;

namespace MarketApp.Gui.Views;

public partial class DashboardPage : ContentPage
{
    public DashboardPage(MainViewModel vm)
    {
        InitializeComponent();
        BindingContext = vm;
    }
}
