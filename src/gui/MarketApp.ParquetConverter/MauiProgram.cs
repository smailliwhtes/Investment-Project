using MarketApp.Gui.Core;
using MarketApp.Gui.Pages;
using MarketApp.Gui.Services;

namespace MarketApp.ParquetConverter;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder.UseMauiApp<App>();

        builder.Services.AddSingleton<IUserSettingsService, UserSettingsService>();
        builder.Services.AddSingleton<IEngineBridgeService, EngineBridgeService>();
        builder.Services.AddSingleton<IFolderPickerService, WindowsFolderPickerService>();
        builder.Services.AddSingleton<ParquetConverterViewModel>();
        builder.Services.AddSingleton<ParquetConverterPage>();

        return builder.Build();
    }
}
