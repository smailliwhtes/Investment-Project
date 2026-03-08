using System.Text.Json;
using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class SampleDataServiceTests
{
    private static readonly object EnvLock = new();

    [Fact]
    public void Dashboard_IncludesLastDateAndLag()
    {
        var service = new SampleDataService();

        var dashboard = service.GetDashboard();

        Assert.NotNull(dashboard.LastRun.LastDateMax);
        Assert.True(dashboard.LastRun.WorstLagDays >= 0);
        Assert.NotEmpty(dashboard.TopSymbols);
        Assert.All(dashboard.TopSymbols, s => Assert.False(string.IsNullOrWhiteSpace(s.LastDate)));
    }

    [Fact]
    public async Task RunViewModel_UsesEngineBridgeProgressEvents()
    {
        var events = new[]
        {
            new ProgressEvent(
                Type: "stage_progress",
                Stage: "run",
                Message: "Processing",
                Pct: 0.5,
                Timestamp: DateTime.UtcNow),
            new ProgressEvent(
                Type: "stage_end",
                Stage: "run",
                Message: "Run completed",
                Pct: 1.0,
                Timestamp: DateTime.UtcNow),
        };

        var vm = new RunViewModel(new FakeEngineBridgeService(events), new FakeUserSettingsService());
        vm.StartCommand.Execute(null);

        await Task.Delay(150);

        Assert.True(vm.Progress >= 0.5);
        Assert.NotEmpty(vm.ProgressEvents);
        Assert.Contains(vm.ProgressEvents, e => e.Message == "Run completed");
    }

    [Fact]
    public void Dashboard_LoadsCauseEffectAndBacktestArtifacts()
    {
        lock (EnvLock)
        {
            var tempRoot = Path.Combine(Path.GetTempPath(), "marketapp_gui_test_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempRoot);
            var previous = Environment.GetEnvironmentVariable("MARKETAPP_OUTPUTS_DIR");

            try
            {
                var runDir = Path.Combine(tempRoot, "runs", "run_test");
                Directory.CreateDirectory(runDir);
                Directory.CreateDirectory(Path.Combine(runDir, "logs"));
                File.WriteAllText(Path.Combine(runDir, "scored.csv"),
                    "symbol,score,rank,gates_passed,flags_count,theme_labels,last_date,lag_days\nAAA,0.9,1,yes,1,AI,2025-01-31,2\n");
                File.WriteAllText(Path.Combine(runDir, "eligible.csv"),
                    "symbol,eligible\nAAA,1\n");
                File.WriteAllText(Path.Combine(runDir, "ui_engine.log"), "[info] test log line\n");

                var manifest = new
                {
                    run_id = "run_test",
                    started_at = "2025-01-31T08:00:00Z",
                    finished_at = "2025-01-31T08:05:00Z",
                    counts = new { universe_count = 1, eligible_count = 1 },
                    data_freshness = new { worst_lag_days = 2, median_lag_days = 1.5, last_date_max = "2025-01-31" }
                };
                File.WriteAllText(
                    Path.Combine(runDir, "run_manifest.json"),
                    JsonSerializer.Serialize(manifest));

                var linkedDir = Path.Combine(runDir, "corpus_linked");
                Directory.CreateDirectory(linkedDir);
                var causeEffect = new
                {
                    counts = new { market_rows = 50, gdelt_rows = 10, joined_rows = 40, event_impact_rows = 12 },
                    top_context_days = new[]
                    {
                        new { day = "2025-01-30", metric = "conflict_event_count_total", value = 7.0 }
                    }
                };
                File.WriteAllText(
                    Path.Combine(linkedDir, "cause_effect_summary.json"),
                    JsonSerializer.Serialize(causeEffect));

                var evalDir = Path.Combine(runDir, "eval");
                Directory.CreateDirectory(evalDir);
                File.WriteAllText(Path.Combine(evalDir, "eval_metrics.csv"),
                    "split,model,mse,mae,accuracy,f1\nfold_1,market_only,0.12,0.08,0.55,0.50\nfold_1,market_plus_corpus,0.10,0.07,0.60,0.56\n");

                Environment.SetEnvironmentVariable("MARKETAPP_OUTPUTS_DIR", tempRoot);
                var service = new SampleDataService();
                var dashboard = service.GetDashboard();

                Assert.NotNull(dashboard.CauseEffect);
                Assert.True(dashboard.CauseEffect!.JoinedRows > 0);
                Assert.NotEmpty(dashboard.BacktestMetrics);
                Assert.Contains(dashboard.BacktestMetrics, row => row.Model == "market_plus_corpus");
                Assert.NotNull(dashboard.QualitySnapshot);
                Assert.Equal("run_test", dashboard.QualitySnapshot!.RunId);
                Assert.NotEmpty(dashboard.QualitySnapshot.Metrics);
            }
            finally
            {
                Environment.SetEnvironmentVariable("MARKETAPP_OUTPUTS_DIR", previous);
                if (Directory.Exists(tempRoot))
                {
                    Directory.Delete(tempRoot, recursive: true);
                }
            }
        }
    }

    private sealed class FakeEngineBridgeService : IEngineBridgeService
    {
        private readonly IReadOnlyList<ProgressEvent> _events;

        public FakeEngineBridgeService(IReadOnlyList<ProgressEvent> events)
        {
            _events = events;
        }

        public async IAsyncEnumerable<ProgressEvent> RunAsync(EngineRunRequest request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var evt in _events)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return evt;
            }
        }

        public Task<ConfigValidationResult> ValidateConfigAsync(string configPath, string? pythonPath, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new ConfigValidationResult(true, Array.Empty<ConfigValidationIssue>(), "{\"valid\":true,\"errors\":[]}"));
        }

        public Task<EngineCommandResult> BuildLinkedAsync(string configPath, string outDirectory, string? pythonPath, bool includeRawGdelt, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new EngineCommandResult(0, string.Empty, string.Empty));
        }

        public Task<EngineCommandResult> EvaluateAsync(string configPath, string outDirectory, string? pythonPath, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new EngineCommandResult(0, string.Empty, string.Empty));
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
            return Task.FromResult(new EngineCommandResult(0, string.Empty, string.Empty));
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
            return Task.FromResult(new EngineCommandResult(0, string.Empty, string.Empty));
        }

        public Task<RunDiffResult> DiffRunsAsync(string runA, string runB, string? pythonPath, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new RunDiffResult("a", "b", new RunDiffSummary(0, 0, 0, 0), Array.Empty<RunDiffRow>()));
        }
    }

    private sealed class FakeUserSettingsService : IUserSettingsService
    {
        private string? _pythonPath;

        public string? GetPythonPath() => _pythonPath;

        public void SetPythonPath(string? pythonPath)
        {
            _pythonPath = pythonPath;
        }
    }
}

