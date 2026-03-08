using Microsoft.UI.Xaml;

namespace MarketApp.Gui.WinUI;

public partial class App : MauiWinUIApplication
{
    public App()
    {
        UnhandledException += OnUnhandledException;
        AppDomain.CurrentDomain.UnhandledException += OnCurrentDomainUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

        InitializeComponent();
    }

    protected override MauiApp CreateMauiApp() => MauiProgram.CreateMauiApp();

    private void OnUnhandledException(object sender, Microsoft.UI.Xaml.UnhandledExceptionEventArgs e)
    {
        WriteStartupError("WinUI unhandled exception", e.Exception);
    }

    private void OnCurrentDomainUnhandledException(object? sender, System.UnhandledExceptionEventArgs e)
    {
        WriteStartupError("AppDomain unhandled exception", e.ExceptionObject as Exception);
    }

    private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        WriteStartupError("TaskScheduler unobserved exception", e.Exception);
        e.SetObserved();
    }

    private static void WriteStartupError(string context, Exception? exception)
    {
        try
        {
            var logPath = Path.Combine(Path.GetTempPath(), "marketapp_winui_startup_error.log");
            var payload = $"{DateTimeOffset.UtcNow:o} {context}{Environment.NewLine}{exception}{Environment.NewLine}{Environment.NewLine}";
            File.AppendAllText(logPath, payload);
        }
        catch
        {
            // Ignore startup log failures.
        }
    }
}
