using MarketApp.Gui.Pages;

namespace MarketApp.ParquetConverter;

public partial class App : Application
{
    public App(ParquetConverterPage converterPage)
    {
        InitializeComponent();

        try
        {
            MainPage = converterPage;
        }
        catch (Exception ex)
        {
            MainPage = BuildStartupErrorPage(ex);
        }
    }

    private static ContentPage BuildStartupErrorPage(Exception ex)
    {
        return new ContentPage
        {
            Title = "Startup Error",
            Content = new ScrollView
            {
                Content = new VerticalStackLayout
                {
                    Padding = 16,
                    Spacing = 10,
                    Children =
                    {
                        new Label
                        {
                            Text = "The Parquet converter failed to initialize.",
                            FontAttributes = FontAttributes.Bold,
                            FontSize = 20,
                        },
                        new Label
                        {
                            Text = ex.ToString(),
                            FontFamily = "Consolas",
                            FontSize = 12,
                        },
                    },
                },
            },
        };
    }
}
