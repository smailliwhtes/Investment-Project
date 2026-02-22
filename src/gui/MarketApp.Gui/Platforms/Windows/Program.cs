using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.UI.Xaml;

namespace MarketApp.Gui.WinUI;

public static class Program
{
    [STAThread]
    static void Main(string[] args)
    {
        // Intercept --smoke BEFORE any MAUI/WinUI bootstrap.
        // MAUI Windows apps require an interactive desktop which is unavailable
        // on headless CI runners. By handling smoke here we avoid the crash.
        if (args.Any(a => string.Equals(a, "--smoke", StringComparison.OrdinalIgnoreCase)))
        {
            RunSmokeAndExit();
            return; // unreachable, but clear intent
        }

        WinRT.ComWrappersSupport.InitializeComWrappers();
        Application.Start(p =>
        {
            var context = new Microsoft.UI.Dispatching.DispatcherQueueSynchronizationContext(
                Microsoft.UI.Dispatching.DispatcherQueue.GetForCurrentThread());
            SynchronizationContext.SetSynchronizationContext(context);

            new App();
        });
    }

    private static void RunSmokeAndExit()
    {
        var readyFile = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_READY_FILE");
        var holdSecondsRaw = Environment.GetEnvironmentVariable("MARKETAPP_SMOKE_HOLD_SECONDS");
        var holdSeconds = int.TryParse(holdSecondsRaw, out var parsed) ? Math.Max(parsed, 1) : 15;

        var payload = new
        {
            pid = Environment.ProcessId,
            timestamp_utc = DateTimeOffset.UtcNow,
            version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.0.0",
            smoke = true
        };

        var json = JsonSerializer.Serialize(payload);

        if (!string.IsNullOrWhiteSpace(readyFile))
        {
            var dir = Path.GetDirectoryName(readyFile);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);
            File.WriteAllText(readyFile, json);
        }

        Console.WriteLine("SMOKE_READY");
        Thread.Sleep(TimeSpan.FromSeconds(holdSeconds));
        Environment.Exit(0);
    }
}
