using System.Text.Json;

namespace MarketApp.Gui;

public partial class App : Application
{
    private const string StartupDiagLogName = "marketapp_startup_diag.log";
    private const string StartupErrorLogName = "marketapp_startup_error.log";

    private readonly bool _smokeMode;

    public App(AppShell shell)
    {
        InitializeComponent();

        try
        {
            var diagPath = Path.Combine(Path.GetTempPath(), StartupDiagLogName);
            var argDump = string.Join(" | ", Environment.GetCommandLineArgs());
            var smokeEnv = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_MODE") ?? "<null>";
            File.AppendAllText(diagPath, $"{DateTimeOffset.UtcNow:o} smoke_env={smokeEnv} args={argDump}{Environment.NewLine}");
        }
        catch
        {
            // Ignore diagnostic logging errors.
        }

        _smokeMode = IsSmokeMode();
        if (_smokeMode)
        {
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
            _ = RunSmokeModeAsync().ContinueWith(t =>
            {
                WriteSmokeError(t.Exception?.ToString() ?? "Smoke mode failed");
                Environment.Exit(1);
            }, TaskContinuationOptions.OnlyOnFaulted);
            return;
        }

        try
        {
            MainPage = shell;
        }
        catch (Exception ex)
        {
            WriteStartupError("MainPage initialization failed", ex);
            MainPage = BuildStartupErrorPage(ex);
        }
    }

    private static bool IsSmokeMode()
    {
        var smokeEnv = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_MODE");
        if (string.Equals(smokeEnv, "1", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(smokeEnv, "true", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return Environment.GetCommandLineArgs()
            .Any(a => string.Equals(a, "--smoke", StringComparison.OrdinalIgnoreCase));
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
            smoke = true,
        };

        var json = JsonSerializer.Serialize(payload);
        if (!string.IsNullOrWhiteSpace(readyFile))
        {
            var directory = Path.GetDirectoryName(readyFile);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await File.WriteAllTextAsync(readyFile, json).ConfigureAwait(false);
        }

        Console.WriteLine("SMOKE_READY");
        await Task.Delay(TimeSpan.FromSeconds(holdSeconds)).ConfigureAwait(false);

        // Keep smoke deterministic and host-agnostic.
        Environment.Exit(0);
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

    private static void WriteStartupError(string context, Exception ex)
    {
        try
        {
            var tempPath = Path.GetTempPath();
            var logPath = Path.Combine(tempPath, StartupErrorLogName);
            var payload = $"{DateTimeOffset.UtcNow:o} {context}{Environment.NewLine}{ex}{Environment.NewLine}{Environment.NewLine}";
            File.AppendAllText(logPath, payload);
        }
        catch
        {
            // Ignore logging errors.
        }
    }

    private static ContentPage BuildStartupErrorPage(Exception ex)
    {
        var logPath = Path.Combine(Path.GetTempPath(), StartupErrorLogName);
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
                            Text = "MarketApp failed to initialize.",
                            FontAttributes = FontAttributes.Bold,
                            FontSize = 20,
                        },
                        new Label
                        {
                            Text = "Review the startup log and restart from a normal (non-admin) user shell.",
                        },
                        new Label
                        {
                            Text = $"Startup log: {logPath}",
                            FontSize = 12,
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
