using Microsoft.Extensions.DependencyInjection;
using MarketApp.Gui.Pages;

namespace MarketApp.Gui.Services;

public sealed class ParquetConverterWindowLauncher
{
    private readonly IServiceProvider _services;

    public ParquetConverterWindowLauncher(IServiceProvider services)
    {
        _services = services;
    }

    public void OpenWindow()
    {
        var page = _services.GetRequiredService<ParquetConverterPage>();
        var window = new Window(page)
        {
            Title = "Folder To Parquet Converter",
            Width = 940,
            Height = 760,
        };
        Application.Current?.OpenWindow(window);
    }
}
