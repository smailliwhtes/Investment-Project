using System.Text.Json;

namespace MarketApp.Gui;

public partial class App : Application
{
    private readonly bool _smokeMode;

    public App(AppShell shell)
    {
        InitializeComponent();
        MainPage = shell;

        _smokeMode = Environment.GetCommandLineArgs().Any(a => string.Equals(a, "--smoke", StringComparison.OrdinalIgnoreCase));
        if (_smokeMode)
        {
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
            _ = RunSmokeModeAsync().ContinueWith(t =>
            {
                WriteSmokeError(t.Exception?.ToString() ?? "Smoke mode failed");
                Environment.Exit(1);
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
    }

    private async Task RunSmokeModeAsync()
    {
        var readyFile = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_READY_FILE");
        var holdSecondsRaw = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_HOLD_SECONDS");
        var holdSeconds = int.TryParse(holdSecondsRaw, out var parsed) ? Math.Max(parsed, 1) : 15;

        var payload = new
        {
            pid = Environment.ProcessId,
            timestamp_utc = DateTimeOffset.UtcNow,
            version = typeof(App).Assembly.GetName().Version?.ToString() ?? "0.0.0",
            smoke = true
        };

        var json = JsonSerializer.Serialize(payload);
        if (!string.IsNullOrWhiteSpace(readyFile))
        {
            var directory = Path.GetDirectoryName(readyFile);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }
            await File.WriteAllTextAsync(readyFile, json);
        }

        Console.WriteLine("SMOKE_READY");
        await Task.Delay(TimeSpan.FromSeconds(holdSeconds));

        MainThread.BeginInvokeOnMainThread(() =>
        {
            Current?.Quit();
        });
    }

    private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        WriteSmokeError(e.ExceptionObject?.ToString() ?? "Unhandled exception");
        Environment.Exit(1);
    }

    private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        WriteSmokeError(e.Exception?.ToString() ?? "Unobserved task exception");
        e.SetObserved();
        Environment.Exit(1);
    }

    private static void WriteSmokeError(string message)
    {
        var tempPath = Path.GetTempPath();
        var logPath = Path.Combine(tempPath, "marketapp_smoke_error.log");
        File.WriteAllText(logPath, $"{DateTimeOffset.UtcNow:o} {message}");
    }
}
