using WinRT.Interop;
using Windows.Storage.Pickers;
using MarketApp.Gui.Core;
using NativeWindow = Microsoft.UI.Xaml.Window;

namespace MarketApp.Gui.Services;

public sealed class WindowsFolderPickerService : IFolderPickerService
{
    public async Task<string?> PickFolderAsync(
        string title,
        string? initialPath = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var nativeWindow = ResolveNativeWindow();
        if (nativeWindow is null)
        {
            return null;
        }

        _ = title;
        var picker = new FolderPicker
        {
            SuggestedStartLocation = PickerLocationId.ComputerFolder,
        };
        picker.FileTypeFilter.Add("*");
        InitializeWithWindow.Initialize(picker, WindowNative.GetWindowHandle(nativeWindow));

        var folder = await picker.PickSingleFolderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        return folder?.Path;
    }

    private static NativeWindow? ResolveNativeWindow()
    {
        return Application.Current?
            .Windows
            .LastOrDefault(window => window.Handler?.PlatformView is NativeWindow)?
            .Handler?
            .PlatformView as NativeWindow;
    }
}
