using System.Diagnostics;
using System.Text;
using System.Text.Json;
using MarketApp.Gui.Core.Abstractions;
using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Core.Services;

public sealed class EngineBridgeService : IEngineBridge
{
    public async Task<EngineRunResult> RunAsync(EngineRunRequest request, IProgress<EngineProgressEvent> progress, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(request.OutDir);
        var logPath = Path.Combine(request.OutDir, "ui_engine.log");

        var psi = new ProcessStartInfo
        {
            FileName = request.PythonPath,
            WorkingDirectory = Directory.GetCurrentDirectory(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };

        psi.ArgumentList.Add("-m");
        psi.ArgumentList.Add("market_monitor.cli");
        psi.ArgumentList.Add("run");
        psi.ArgumentList.Add("--config");
        psi.ArgumentList.Add(request.ConfigPath);
        psi.ArgumentList.Add("--out-dir");
        psi.ArgumentList.Add(request.OutDir);
        if (request.Offline)
        {
            psi.ArgumentList.Add("--offline");
        }
        if (request.ProgressJsonl)
        {
            psi.ArgumentList.Add("--progress-jsonl");
        }

        psi.Environment["PYTHONUTF8"] = "1";
        psi.Environment["PYTHONIOENCODING"] = "utf-8";

        using var process = new Process { StartInfo = psi };
        process.Start();

        await using var logWriter = File.CreateText(logPath);

        var outTask = Task.Run(async () =>
        {
            while (!process.StandardOutput.EndOfStream)
            {
                var line = await process.StandardOutput.ReadLineAsync(cancellationToken);
                if (line is null) continue;
                await logWriter.WriteLineAsync(line);
                if (TryParseProgress(line, out var evt))
                {
                    progress.Report(evt);
                }
            }
        }, cancellationToken);

        var errTask = Task.Run(async () =>
        {
            while (!process.StandardError.EndOfStream)
            {
                var line = await process.StandardError.ReadLineAsync(cancellationToken);
                if (line is null) continue;
                await logWriter.WriteLineAsync($"[stderr] {line}");
            }
        }, cancellationToken);

        using var reg = cancellationToken.Register(() =>
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        });

        await Task.WhenAll(outTask, errTask);
        await process.WaitForExitAsync(cancellationToken);
        return new EngineRunResult(process.ExitCode, logPath, logPath, request.OutDir);
    }

    public async Task<ConfigValidationResult> ValidateConfigAsync(string configPath, CancellationToken cancellationToken)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "python",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-m");
        psi.ArgumentList.Add("market_monitor.cli");
        psi.ArgumentList.Add("validate-config");
        psi.ArgumentList.Add("--config");
        psi.ArgumentList.Add(configPath);
        psi.ArgumentList.Add("--format");
        psi.ArgumentList.Add("json");

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("Unable to start validation process.");
        var stdout = await process.StandardOutput.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);

        return ValidationParser.Parse(stdout);
    }

    public static bool TryParseProgress(string line, out EngineProgressEvent evt)
    {
        evt = new EngineProgressEvent(DateTime.UtcNow, "unknown", 0, line);
        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            evt = new EngineProgressEvent(
                root.TryGetProperty("ts", out var ts) && DateTime.TryParse(ts.GetString(), out var dt) ? dt : DateTime.UtcNow,
                root.GetProperty("stage").GetString() ?? "unknown",
                root.TryGetProperty("pct", out var pct) ? pct.GetInt32() : 0,
                root.TryGetProperty("message", out var msg) ? (msg.GetString() ?? string.Empty) : string.Empty,
                root.TryGetProperty("artifact", out var artifact) ? artifact.ToString() : null
            );
            return true;
        }
        catch
        {
            return false;
        }
    }
}
