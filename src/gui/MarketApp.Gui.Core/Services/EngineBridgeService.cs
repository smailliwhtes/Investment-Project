using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace MarketApp.Gui.Core;

public sealed class EngineBridgeService : IEngineBridgeService
{
    private static readonly TimeSpan ProgressThrottleInterval = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan PostStageHeartbeatInterval = TimeSpan.FromSeconds(5);

    public async IAsyncEnumerable<ProgressEvent> RunAsync(
        EngineRunRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var runDir = Path.GetFullPath(request.OutDirectory);
        Directory.CreateDirectory(runDir);
        var uiLogPath = Path.Combine(runDir, "ui_engine.log");
        using var logStream = new FileStream(uiLogPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        using var logWriter = new StreamWriter(logStream) { AutoFlush = true };

        var python = ResolvePythonPath(request.PythonPath);
        var marketAppRoot = ResolveMarketAppRoot();
        var runArgs = BuildRunArguments(request, runDir, marketAppRoot);

        var runCompleted = false;
        var runHadError = false;

        await foreach (var evt in StreamCommandAsync(
            python,
            runArgs,
            runDir,
            logWriter,
            cancellationToken))
        {
            if (string.Equals(evt.Stage, "run", StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(evt.Type, "error", StringComparison.OrdinalIgnoreCase))
                {
                    runHadError = true;
                    runCompleted = true;
                }
                else if (string.Equals(evt.Type, "stage_end", StringComparison.OrdinalIgnoreCase))
                {
                    runCompleted = true;
                }
            }

            yield return evt;
        }

        if (!ShouldRunPostRunActions(cancellationToken.IsCancellationRequested, runHadError, runCompleted))
        {
            yield break;
        }

        if (request.RunBuildLinked)
        {
            yield return new ProgressEvent(
                Type: "stage_start",
                Stage: "corpus_build_linked",
                Message: "Running corpus build-linked",
                Pct: null,
                Timestamp: DateTime.UtcNow);

            var linkedOutDir = Path.Combine(runDir, "corpus_linked");
            var linkedTask = BuildLinkedAsync(
                request.ConfigPath,
                linkedOutDir,
                request.PythonPath,
                request.IncludeRawGdelt,
                cancellationToken);

            while (!linkedTask.IsCompleted)
            {
                var heartbeatTask = Task.Delay(PostStageHeartbeatInterval, cancellationToken);
                var completed = await Task.WhenAny(linkedTask, heartbeatTask).ConfigureAwait(false);
                if (completed != linkedTask)
                {
                    yield return new ProgressEvent(
                        Type: "stage_progress",
                        Stage: "corpus_build_linked",
                        Message: "Corpus build-linked still running...",
                        Pct: null,
                        Timestamp: DateTime.UtcNow);
                }
            }

            var linked = await linkedTask.ConfigureAwait(false);

            var linkedType = linked.ExitCode == 0 ? "stage_end" : "error";
            yield return new ProgressEvent(
                Type: linkedType,
                Stage: "corpus_build_linked",
                Message: linked.ExitCode == 0 ? "Corpus linkage complete" : "Corpus linkage failed",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: linked.ExitCode == 0 ? null : new ProgressError("RUNTIME_FAILURE", linked.Stderr.Trim(), null));

            if (linked.ExitCode != 0)
            {
                yield return new ProgressEvent(
                    Type: "warning",
                    Stage: "evaluate",
                    Message: "Skipping evaluation because corpus build-linked failed.",
                    Pct: null,
                    Timestamp: DateTime.UtcNow);
                yield break;
            }
        }

        if (request.RunEvaluate)
        {
            yield return new ProgressEvent(
                Type: "stage_start",
                Stage: "evaluate",
                Message: "Running backtest/forecast quality evaluation",
                Pct: null,
                Timestamp: DateTime.UtcNow);

            var evalTask = EvaluateAsync(
                request.ConfigPath,
                runDir,
                request.PythonPath,
                cancellationToken);

            while (!evalTask.IsCompleted)
            {
                var heartbeatTask = Task.Delay(PostStageHeartbeatInterval, cancellationToken);
                var completed = await Task.WhenAny(evalTask, heartbeatTask).ConfigureAwait(false);
                if (completed != evalTask)
                {
                    yield return new ProgressEvent(
                        Type: "stage_progress",
                        Stage: "evaluate",
                        Message: "Evaluation still running...",
                        Pct: null,
                        Timestamp: DateTime.UtcNow);
                }
            }

            var eval = await evalTask.ConfigureAwait(false);

            var evalType = eval.ExitCode == 0 ? "stage_end" : "error";
            yield return new ProgressEvent(
                Type: evalType,
                Stage: "evaluate",
                Message: eval.ExitCode == 0 ? "Evaluation complete" : "Evaluation failed",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: eval.ExitCode == 0 ? null : new ProgressError("RUNTIME_FAILURE", eval.Stderr.Trim(), null));
        }
    }

    public async Task<ConfigValidationResult> ValidateConfigAsync(
        string configPath,
        string? pythonPath,
        CancellationToken cancellationToken = default)
    {
        var marketAppRoot = ResolveMarketAppRoot();
        var resolvedConfigPath = ResolveConfigPath(configPath, marketAppRoot);
        var python = ResolvePythonPath(pythonPath);
        var result = await RunCommandCaptureAsync(
            python,
            new[]
            {
                "-m", "market_monitor.cli", "validate-config",
                "--config", resolvedConfigPath,
                "--format", "json"
            },
            marketAppRoot,
            cancellationToken).ConfigureAwait(false);

        var json = ExtractJsonPayload(result.Stdout) ?? "{\"valid\":false,\"errors\":[{\"path\":\"\",\"message\":\"Validation output missing JSON\",\"severity\":\"error\"}]}";
        var errors = new List<ConfigValidationIssue>();
        var valid = false;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("valid", out var validNode) && validNode.ValueKind == JsonValueKind.True)
            {
                valid = true;
            }

            if (root.TryGetProperty("errors", out var errorNode) && errorNode.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in errorNode.EnumerateArray())
                {
                    errors.Add(new ConfigValidationIssue(
                        GetString(item, "path") ?? string.Empty,
                        GetString(item, "message") ?? string.Empty,
                        GetString(item, "severity") ?? "error"));
                }
            }
        }
        catch (JsonException)
        {
            errors.Add(new ConfigValidationIssue("", "Failed to parse validation JSON", "error"));
            valid = false;
        }

        return new ConfigValidationResult(valid, errors, json);
    }

    public Task<EngineCommandResult> BuildLinkedAsync(
        string configPath,
        string outDirectory,
        string? pythonPath,
        bool includeRawGdelt,
        CancellationToken cancellationToken = default)
    {
        var marketAppRoot = ResolveMarketAppRoot();
        var args = new List<string>
        {
            "-m", "market_monitor.cli", "corpus", "build-linked",
            "--config", ResolveConfigPath(configPath, marketAppRoot),
            "--outdir", Path.GetFullPath(outDirectory)
        };
        if (includeRawGdelt)
        {
            args.Add("--include-raw-gdelt");
        }

        return RunCommandCaptureAsync(
            ResolvePythonPath(pythonPath),
            args,
            marketAppRoot,
            cancellationToken);
    }

    public Task<EngineCommandResult> EvaluateAsync(
        string configPath,
        string outDirectory,
        string? pythonPath,
        CancellationToken cancellationToken = default)
    {
        var marketAppRoot = ResolveMarketAppRoot();
        return RunCommandCaptureAsync(
            ResolvePythonPath(pythonPath),
            new[]
            {
                "-m", "market_monitor.cli", "evaluate",
                "--config", ResolveConfigPath(configPath, marketAppRoot),
                "--outdir", Path.GetFullPath(outDirectory),
                "--offline"
            },
            marketAppRoot,
            cancellationToken);
    }


    public Task<EngineCommandResult> ImportOhlcvAsync(
        string sourceDirectory,
        string destinationDirectory,
        string? pythonPath,
        bool normalize = true,
        string? dateColumn = null,
        string? delimiter = null,
        CancellationToken cancellationToken = default)
    {
        var args = new List<string>
        {
            "-m", "market_monitor.cli", "provision", "import-ohlcv",
            "--src", Path.GetFullPath(sourceDirectory),
            "--dest", Path.GetFullPath(destinationDirectory)
        };

        if (normalize)
        {
            args.Add("--normalize");
        }

        if (!string.IsNullOrWhiteSpace(dateColumn))
        {
            args.Add("--date-col");
            args.Add(dateColumn);
        }

        if (!string.IsNullOrWhiteSpace(delimiter))
        {
            args.Add("--delimiter");
            args.Add(delimiter);
        }

        return RunCommandCaptureAsync(
            ResolvePythonPath(pythonPath),
            args,
            ResolveMarketAppRoot(),
            cancellationToken);
    }

    public Task<EngineCommandResult> ImportExogenousAsync(
        string sourceDirectory,
        string destinationDirectory,
        string? pythonPath,
        bool normalize = true,
        string? normalizedDestinationDirectory = null,
        string fileGlob = "*.csv",
        string formatHint = "auto",
        string writeFormat = "csv",
        CancellationToken cancellationToken = default)
    {
        var args = new List<string>
        {
            "-m", "market_monitor.cli", "provision", "import-exogenous",
            "--src", Path.GetFullPath(sourceDirectory),
            "--dest", Path.GetFullPath(destinationDirectory),
            "--glob", string.IsNullOrWhiteSpace(fileGlob) ? "*.csv" : fileGlob,
            "--format", string.IsNullOrWhiteSpace(formatHint) ? "auto" : formatHint,
            "--write", string.IsNullOrWhiteSpace(writeFormat) ? "csv" : writeFormat,
        };

        if (normalize)
        {
            args.Add("--normalize");
        }

        if (!string.IsNullOrWhiteSpace(normalizedDestinationDirectory))
        {
            args.Add("--normalized-dest");
            args.Add(Path.GetFullPath(normalizedDestinationDirectory));
        }

        return RunCommandCaptureAsync(
            ResolvePythonPath(pythonPath),
            args,
            ResolveMarketAppRoot(),
            cancellationToken);
    }
    public async Task<RunDiffResult> DiffRunsAsync(
        string runA,
        string runB,
        string? pythonPath,
        CancellationToken cancellationToken = default)
    {
        var result = await RunCommandCaptureAsync(
            ResolvePythonPath(pythonPath),
            new[]
            {
                "-m", "market_monitor.cli", "diff-runs",
                "--run-a", Path.GetFullPath(runA),
                "--run-b", Path.GetFullPath(runB),
                "--format", "json"
            },
            ResolveMarketAppRoot(),
            cancellationToken).ConfigureAwait(false);

        var json = ExtractJsonPayload(result.Stdout);
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new InvalidOperationException("diff-runs did not return JSON output.");
        }

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var summary = root.GetProperty("summary");
        var diffSummary = new RunDiffSummary(
            NSymbols: GetInt(summary, "n_symbols") ?? 0,
            NNew: GetInt(summary, "n_new") ?? 0,
            NRemoved: GetInt(summary, "n_removed") ?? 0,
            NRankChanged: GetInt(summary, "n_rank_changed") ?? 0);

        var rows = new List<RunDiffRow>();
        if (root.TryGetProperty("rows", out var rowsNode) && rowsNode.ValueKind == JsonValueKind.Array)
        {
            foreach (var row in rowsNode.EnumerateArray())
            {
                var drivers = new List<string>();
                if (row.TryGetProperty("drivers", out var driversNode) && driversNode.ValueKind == JsonValueKind.Array)
                {
                    foreach (var driver in driversNode.EnumerateArray())
                    {
                        if (driver.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(driver.GetString()))
                        {
                            drivers.Add(driver.GetString()!);
                        }
                    }
                }

                rows.Add(new RunDiffRow(
                    Symbol: GetString(row, "symbol") ?? string.Empty,
                    RankA: GetNullableInt(row, "rank_a"),
                    RankB: GetNullableInt(row, "rank_b"),
                    ScoreA: GetNullableDouble(row, "score_a"),
                    ScoreB: GetNullableDouble(row, "score_b"),
                    DeltaScore: GetNullableDouble(row, "delta_score"),
                    DeltaRank: GetNullableInt(row, "delta_rank"),
                    FlagsA: GetNullableInt(row, "flags_a"),
                    FlagsB: GetNullableInt(row, "flags_b"),
                    Drivers: drivers));
            }
        }

        return new RunDiffResult(
            RunA: GetString(root, "run_a") ?? Path.GetFileName(runA),
            RunB: GetString(root, "run_b") ?? Path.GetFileName(runB),
            Summary: diffSummary,
            Rows: rows);
    }

    private async IAsyncEnumerable<ProgressEvent> StreamCommandAsync(
        string python,
        IReadOnlyList<string> args,
        string runDir,
        StreamWriter logWriter,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var marketAppRoot = ResolveMarketAppRoot();
        var startInfo = BuildStartInfo(python, args, marketAppRoot);

        using var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        var started = false;
        string? startError = null;
        try
        {
            started = process.Start();
        }
        catch (Exception ex)
        {
            startError = ex.Message;
        }

        if (!started)
        {
            var detail = string.IsNullOrWhiteSpace(startError)
                ? "Unable to start process"
                : startError;
            yield return new ProgressEvent(
                Type: "error",
                Stage: "run",
                Message: "Failed to start engine process",
                Pct: null,
                Timestamp: DateTime.UtcNow,
                Error: new ProgressError("RUNTIME_FAILURE", detail, null));
            yield break;
        }

        var channel = Channel.CreateUnbounded<ProgressEvent>();
        var lockObj = new object();
        var lastProgress = DateTime.MinValue;

        var stdoutTask = Task.Run(async () =>
        {
            await PumpStreamAsync(
                process.StandardOutput,
                "stdout",
                logWriter,
                channel.Writer,
                lockObj,
                () => lastProgress,
                v => lastProgress = v,
                cancellationToken).ConfigureAwait(false);
        }, cancellationToken);

        var stderrTask = Task.Run(async () =>
        {
            await PumpStreamAsync(
                process.StandardError,
                "stderr",
                logWriter,
                channel.Writer,
                lockObj,
                () => lastProgress,
                v => lastProgress = v,
                cancellationToken).ConfigureAwait(false);
        }, cancellationToken);

        using var registration = cancellationToken.Register(() =>
        {
            TryKillProcessTree(process);
        });

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.WhenAll(stdoutTask, stderrTask).ConfigureAwait(false);
                await process.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);

                var exitType = process.ExitCode switch
                {
                    0 => "stage_end",
                    130 => "error",
                    _ => "error"
                };

                var exitMessage = process.ExitCode switch
                {
                    0 => "Run completed",
                    130 => "Run canceled",
                    _ => $"Run failed (exit {process.ExitCode})"
                };

                await channel.Writer.WriteAsync(new ProgressEvent(
                    Type: exitType,
                    Stage: "run",
                    Message: exitMessage,
                    Pct: process.ExitCode == 0 ? 1.0 : null,
                    Timestamp: DateTime.UtcNow,
                    Error: process.ExitCode is 0
                        ? null
                        : new ProgressError(process.ExitCode == 130 ? "INTERRUPTED" : "RUNTIME_FAILURE", exitMessage, null))).ConfigureAwait(false);
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        }, CancellationToken.None);

        while (await channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (channel.Reader.TryRead(out var evt))
            {
                yield return evt;
            }
        }
    }

    private static async Task PumpStreamAsync(
        StreamReader reader,
        string source,
        StreamWriter logWriter,
        ChannelWriter<ProgressEvent> writer,
        object lockObj,
        Func<DateTime> getLastProgress,
        Action<DateTime> setLastProgress,
        CancellationToken cancellationToken)
    {
        while (!reader.EndOfStream && !cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                continue;
            }

            await logWriter.WriteLineAsync($"[{DateTime.UtcNow:O}] {source}: {line}").ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            if (TryParseProgressEvent(line, out var evt))
            {
                var shouldEmit = true;
                if (string.Equals(evt.Type, "stage_progress", StringComparison.OrdinalIgnoreCase))
                {
                    lock (lockObj)
                    {
                        var now = DateTime.UtcNow;
                        if ((now - getLastProgress()) < ProgressThrottleInterval)
                        {
                            shouldEmit = false;
                        }
                        else
                        {
                            setLastProgress(now);
                        }
                    }
                }

                if (shouldEmit)
                {
                    await writer.WriteAsync(evt, cancellationToken).ConfigureAwait(false);
                }

                continue;
            }

            await writer.WriteAsync(new ProgressEvent(
                Type: "warning",
                Stage: source,
                Message: line,
                Pct: null,
                Timestamp: DateTime.UtcNow,
                RawLine: line), cancellationToken).ConfigureAwait(false);
        }
    }

    internal static IReadOnlyList<string> BuildRunArguments(EngineRunRequest request, string runDir, string? marketAppRootOverride = null)
    {
        var marketAppRoot = marketAppRootOverride ?? ResolveMarketAppRoot();
        var args = new List<string>
        {
            "-m", "market_monitor.cli", "run",
            "--config", ResolveConfigPath(request.ConfigPath, marketAppRoot),
            "--out-dir", Path.GetFullPath(runDir),
            "--progress-jsonl"
        };
        if (request.Offline)
        {
            args.Add("--offline");
        }
        if (!string.IsNullOrWhiteSpace(request.WatchlistPath))
        {
            args.Add("--watchlist");
            args.Add(ResolveEnginePath(request.WatchlistPath, marketAppRoot));
        }
        if (request.IncludeRawGdelt)
        {
            args.Add("--include-raw-gdelt");
        }

        return args;
    }

    internal static bool ShouldRunPostRunActions(bool cancellationRequested, bool runHadError, bool runCompleted)
    {
        return !cancellationRequested && runCompleted && !runHadError;
    }
    internal static ProcessStartInfo BuildStartInfo(string python, IReadOnlyList<string> args, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = python,
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

        startInfo.Environment["PYTHONUTF8"] = "1";
        startInfo.Environment["PYTHONIOENCODING"] = "utf-8";

        foreach (var arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        return startInfo;
    }

    private static async Task<EngineCommandResult> RunCommandCaptureAsync(
        string python,
        IEnumerable<string> args,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        var argList = args.ToList();
        var startInfo = BuildStartInfo(python, argList, workingDirectory);
        using var process = new Process { StartInfo = startInfo };

        process.Start();

        var stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);

        try
        {
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            TryKillProcessTree(process);
            throw;
        }

        var stdout = await stdoutTask.ConfigureAwait(false);
        var stderr = await stderrTask.ConfigureAwait(false);
        return new EngineCommandResult(process.ExitCode, stdout, stderr);
    }

    internal static bool TryParseProgressEvent(string line, out ProgressEvent evt)
    {
        evt = default!;
        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;
            var type = GetString(root, "type");
            var stage = GetString(root, "stage");
            var message = GetString(root, "message");
            if (string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(stage) || string.IsNullOrWhiteSpace(message))
            {
                return false;
            }

            var timestamp = ParseTimestamp(GetString(root, "ts"));
            var pct = GetNullableDouble(root, "pct");

            ProgressCounters? counters = null;
            if (root.TryGetProperty("counters", out var countersNode) && countersNode.ValueKind == JsonValueKind.Object)
            {
                counters = new ProgressCounters(
                    Done: GetInt(countersNode, "done") ?? 0,
                    Total: GetInt(countersNode, "total") ?? 0,
                    Units: GetString(countersNode, "units") ?? string.Empty);
            }

            ProgressArtifact? artifact = null;
            if (root.TryGetProperty("artifact", out var artifactNode) && artifactNode.ValueKind == JsonValueKind.Object)
            {
                artifact = new ProgressArtifact(
                    Name: GetString(artifactNode, "name") ?? string.Empty,
                    Path: GetString(artifactNode, "path") ?? string.Empty,
                    Rows: GetNullableInt(artifactNode, "rows"),
                    Hash: GetString(artifactNode, "hash"));
            }

            ProgressError? error = null;
            if (root.TryGetProperty("error", out var errorNode) && errorNode.ValueKind == JsonValueKind.Object)
            {
                error = new ProgressError(
                    Code: GetString(errorNode, "code") ?? string.Empty,
                    Detail: GetString(errorNode, "detail") ?? string.Empty,
                    Traceback: GetString(errorNode, "traceback"));
            }

            evt = new ProgressEvent(type, stage, message, pct, timestamp, counters, artifact, error, line);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static DateTime ParseTimestamp(string? value)
    {
        if (DateTimeOffset.TryParse(value, out var parsed))
        {
            return parsed.UtcDateTime;
        }

        return DateTime.UtcNow;
    }

    private static void TryKillProcessTree(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
        catch
        {
            // Best effort cancellation.
        }
    }

    private static string? ExtractJsonPayload(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var lines = text
            .Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Trim())
            .ToArray();

        for (var i = lines.Length - 1; i >= 0; i--)
        {
            if (lines[i].StartsWith("{", StringComparison.Ordinal) && lines[i].EndsWith("}", StringComparison.Ordinal))
            {
                return lines[i];
            }
        }

        var start = text.IndexOf('{');
        var end = text.LastIndexOf('}');
        if (start >= 0 && end > start)
        {
            return text[start..(end + 1)];
        }

        return null;
    }

    internal static string ResolvePythonPath(string? preferredPath, string? repoRootOverride = null)
    {
        if (!string.IsNullOrWhiteSpace(preferredPath) && File.Exists(preferredPath))
        {
            return Path.GetFullPath(preferredPath);
        }

        var repoRoot = repoRootOverride ?? ResolveRepoRoot();
        var candidates = new[]
        {
            Path.Combine(repoRoot, ".venv", "Scripts", "python.exe"),
            Path.Combine(repoRoot, "market_app", ".venv", "Scripts", "python.exe")
        };
        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return "python";
    }

    internal static string ResolveConfigPath(string configPath, string marketAppRoot, string? repoRootOverride = null)
    {
        if (string.IsNullOrWhiteSpace(configPath))
        {
            return configPath;
        }

        if (Path.IsPathRooted(configPath))
        {
            return Path.GetFullPath(configPath);
        }

        var repoRoot = repoRootOverride ?? ResolveRepoRoot();
        var repoCandidate = Path.GetFullPath(configPath, repoRoot);
        if (File.Exists(repoCandidate))
        {
            return repoCandidate;
        }

        var marketAppCandidate = ResolveEnginePath(configPath, marketAppRoot, repoRoot);
        if (File.Exists(marketAppCandidate))
        {
            return marketAppCandidate;
        }

        if (!configPath.Contains(Path.DirectorySeparatorChar) &&
            !configPath.Contains(Path.AltDirectorySeparatorChar))
        {
            var configDirCandidate = Path.GetFullPath(Path.Combine(marketAppRoot, "config", configPath));
            if (File.Exists(configDirCandidate))
            {
                return configDirCandidate;
            }
        }

        return marketAppCandidate;
    }

    internal static string ResolveEnginePath(string path, string marketAppRoot, string? repoRootOverride = null)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return path;
        }

        if (Path.IsPathRooted(path))
        {
            return Path.GetFullPath(path);
        }

        var repoRoot = repoRootOverride ?? ResolveRepoRoot();
        var repoCandidate = Path.GetFullPath(path, repoRoot);
        if (File.Exists(repoCandidate) || Directory.Exists(repoCandidate))
        {
            return repoCandidate;
        }

        return Path.GetFullPath(path, marketAppRoot);
    }

    private static string ResolveMarketAppRoot()
    {
        var repoRoot = ResolveRepoRoot();
        var marketAppRoot = Path.Combine(repoRoot, "market_app");
        if (!Directory.Exists(marketAppRoot))
        {
            throw new DirectoryNotFoundException("Unable to locate market_app directory from GUI runtime.");
        }

        return marketAppRoot;
    }

    private static string ResolveRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        for (var depth = 0; depth < 12 && current is not null; depth++)
        {
            var marker = Path.Combine(current.FullName, "market_app", "market_monitor", "cli.py");
            if (File.Exists(marker))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return Directory.GetCurrentDirectory();
    }

    private static string? GetString(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var node))
        {
            return null;
        }

        return node.ValueKind switch
        {
            JsonValueKind.String => node.GetString(),
            JsonValueKind.Number => node.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private static int? GetInt(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var node))
        {
            return null;
        }

        if (node.ValueKind == JsonValueKind.Number && node.TryGetInt32(out var number))
        {
            return number;
        }

        if (node.ValueKind == JsonValueKind.String && int.TryParse(node.GetString(), out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static int? GetNullableInt(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var node) || node.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (node.ValueKind == JsonValueKind.Number && node.TryGetInt32(out var parsed))
        {
            return parsed;
        }

        if (node.ValueKind == JsonValueKind.String && int.TryParse(node.GetString(), out parsed))
        {
            return parsed;
        }

        return null;
    }

    private static double? GetNullableDouble(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var node) || node.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (node.ValueKind == JsonValueKind.Number && node.TryGetDouble(out var value))
        {
            return value;
        }

        if (node.ValueKind == JsonValueKind.String && double.TryParse(node.GetString(), out value))
        {
            return value;
        }

        return null;
    }
}

