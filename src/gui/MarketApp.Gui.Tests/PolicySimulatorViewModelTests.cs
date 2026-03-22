using System.Text.Json;
using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class PolicySimulatorViewModelTests
{
    [Fact]
    public async Task RunSimulationAsync_LoadsSummaryFileWhenPresent()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "policy_sim_vm_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var bridge = new RecordingEngineBridgeService(async request =>
            {
                Directory.CreateDirectory(request.OutDirectory);
                var summaryPath = Path.Combine(request.OutDirectory, "policy_simulation_summary.json");
                var payload = JsonSerializer.Serialize(new
                {
                    scenario = request.ScenarioName,
                    status = "complete",
                    summary = "Policy scenario complete",
                    output_dir = request.OutDirectory,
                    fields = new[]
                    {
                        new { name = "median_return", value = "0.012" },
                        new { name = "worst_drawdown", value = "-0.044" }
                    }
                });
                await File.WriteAllTextAsync(summaryPath, payload);
                return new[]
                {
                    new ProgressEvent("stage_start", "policy_loader", "Loading scenario", null, DateTime.UtcNow),
                    new ProgressEvent("stage_progress", "policy_loader", "Computing impacts", 0.5, DateTime.UtcNow),
                    new ProgressEvent("stage_end", "policy_simulate", "Policy simulation completed", 1.0, DateTime.UtcNow),
                };
            });
            var settings = new InMemorySettingsService();
            var viewModel = new PolicySimulatorViewModel(bridge, settings)
            {
                ConfigPath = Path.Combine(tempDir, "config.yaml"),
                ScenarioName = "tariff-shock",
                OutputDirectory = tempDir,
                PythonPath = "python"
            };

            await viewModel.RunSimulationAsync();

            Assert.Equal("Policy simulation complete", viewModel.Status);
            Assert.NotNull(viewModel.SummaryPath);
            Assert.Contains("Policy scenario complete", viewModel.SummaryText);
            Assert.Contains("median_return", viewModel.SummaryText);
            Assert.Contains("Progress log:", viewModel.SummaryText);
            Assert.NotNull(bridge.LastRequest);
            Assert.True(bridge.LastRequest!.Offline);
            Assert.Equal("tariff-shock", bridge.LastRequest.ScenarioName);
            Assert.Equal(1, bridge.StreamCallCount);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunSimulationAsync_IsResilientWhenSummaryFileMissing()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "policy_sim_vm_missing_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var bridge = new RecordingEngineBridgeService(request =>
            {
                Directory.CreateDirectory(request.OutDirectory);
                return Task.FromResult<IReadOnlyList<ProgressEvent>>(new[]
                {
                    new ProgressEvent("stage_start", "policy_loader", "Loading scenario", null, DateTime.UtcNow),
                    new ProgressEvent("stage_end", "policy_simulate", "Policy simulation completed", 1.0, DateTime.UtcNow),
                });
            });
            var settings = new InMemorySettingsService();
            var viewModel = new PolicySimulatorViewModel(bridge, settings)
            {
                ConfigPath = Path.Combine(tempDir, "config.yaml"),
                ScenarioName = "sanctions",
                OutputDirectory = tempDir,
                PythonPath = "python"
            };

            await viewModel.RunSimulationAsync();

            Assert.Equal("Policy simulation complete", viewModel.Status);
            Assert.Null(viewModel.SummaryPath);
            Assert.Contains("No JSON summary file was found", viewModel.SummaryText);
            Assert.Contains("Progress log:", viewModel.SummaryText);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    private sealed class RecordingEngineBridgeService : IEngineBridgeService
    {
        private readonly Func<PolicySimulationRequest, Task<IReadOnlyList<ProgressEvent>>> _simulateHandler;

        public RecordingEngineBridgeService(Func<PolicySimulationRequest, Task<IReadOnlyList<ProgressEvent>>> simulateHandler)
        {
            _simulateHandler = simulateHandler;
        }

        public PolicySimulationRequest? LastRequest { get; private set; }
        public int StreamCallCount { get; private set; }

        public async IAsyncEnumerable<ProgressEvent> RunAsync(
            EngineRunRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ProgressEvent> SimulatePolicyStreamAsync(
            PolicySimulationRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            StreamCallCount++;
            var events = await _simulateHandler(request).ConfigureAwait(false);
            foreach (var evt in events)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return evt;
            }
        }

        public Task<EngineCommandResult> SimulatePolicyAsync(
            PolicySimulationRequest request,
            CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            return Task.FromResult(new EngineCommandResult(0, string.Empty, string.Empty));
        }

        public Task<ConfigValidationResult> ValidateConfigAsync(string configPath, string? pythonPath, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> BuildLinkedAsync(string configPath, string outDirectory, string? pythonPath, bool includeRawGdelt, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> EvaluateAsync(string configPath, string outDirectory, string? pythonPath, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> ImportOhlcvAsync(string sourceDirectory, string destinationDirectory, string? pythonPath, bool normalize = true, string? dateColumn = null, string? delimiter = null, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> ImportExogenousAsync(string sourceDirectory, string destinationDirectory, string? pythonPath, bool normalize = true, string? normalizedDestinationDirectory = null, string fileGlob = "*.csv", string formatHint = "auto", string writeFormat = "csv", CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RunDiffResult> DiffRunsAsync(string runA, string runB, string? pythonPath, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }

    private sealed class InMemorySettingsService : IUserSettingsService
    {
        private string? _pythonPath;

        public string? GetPythonPath() => _pythonPath;

        public void SetPythonPath(string? pythonPath)
        {
            _pythonPath = pythonPath;
        }
    }
}
