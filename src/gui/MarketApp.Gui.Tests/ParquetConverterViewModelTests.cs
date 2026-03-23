using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class ParquetConverterViewModelTests
{
    [Fact]
    public async Task BrowseSourceFolderAsync_UsesFolderPickerSelection()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "parquet_converter_pick_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var folderPicker = new FakeFolderPickerService(tempDir);
            var viewModel = new ParquetConverterViewModel(
                new RecordingEngineBridgeService(),
                folderPicker,
                new InMemorySettingsService());

            await viewModel.BrowseSourceFolderAsync();

            Assert.Equal(tempDir, viewModel.SourceDirectory);
            Assert.Contains("_parquet", viewModel.DefaultOutputDirectoryPreview, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("Source folder selected", viewModel.Status);
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
    public async Task RunConversionAsync_PopulatesSummaryFromBridgeResult()
    {
        var sourceDir = Path.Combine(Path.GetTempPath(), "parquet_converter_source_" + Guid.NewGuid().ToString("N"));
        var outputDir = Path.Combine(Path.GetTempPath(), "parquet_converter_output_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(sourceDir);

        try
        {
            var bridge = new RecordingEngineBridgeService
            {
                Result = new FolderConversionResult(
                    ExitCode: 0,
                    Stdout: string.Empty,
                    Stderr: string.Empty,
                    SourceRoot: sourceDir,
                    OutputDirectory: outputDir,
                    Strict: false,
                    FilesScanned: 7,
                    FilesConverted: 5,
                    FilesSkipped: 2,
                    FilesWithErrors: 0,
                    ManifestPath: Path.Combine(outputDir, "folder_conversion_manifest.json"),
                    InventoryCsvPath: Path.Combine(outputDir, "folder_conversion_inventory.csv"),
                    ReportPath: Path.Combine(outputDir, "folder_conversion_report.md"))
            };
            var viewModel = new ParquetConverterViewModel(
                bridge,
                new FakeFolderPickerService(),
                new InMemorySettingsService());
            viewModel.SourceDirectory = sourceDir;
            viewModel.OutputDirectory = outputDir;

            await viewModel.RunConversionAsync();

            Assert.Equal("Conversion complete", viewModel.Status);
            Assert.Equal(7, viewModel.FilesScanned);
            Assert.Equal(5, viewModel.FilesConverted);
            Assert.Equal(2, viewModel.FilesSkipped);
            Assert.Equal(0, viewModel.FilesWithErrors);
            Assert.Equal(Path.Combine(outputDir, "folder_conversion_manifest.json"), viewModel.ManifestPath);
            Assert.Equal(outputDir, bridge.LastOutputDirectory);
            Assert.False(bridge.LastStrictMode);
        }
        finally
        {
            if (Directory.Exists(sourceDir))
            {
                Directory.Delete(sourceDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunConversionAsync_StrictFailureStillShowsSummary()
    {
        var sourceDir = Path.Combine(Path.GetTempPath(), "parquet_converter_strict_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(sourceDir);

        try
        {
            var bridge = new RecordingEngineBridgeService
            {
                Result = new FolderConversionResult(
                    ExitCode: 4,
                    Stdout: string.Empty,
                    Stderr: "Strict mode blocked conversion because 2 file(s) were skipped.",
                    SourceRoot: sourceDir,
                    OutputDirectory: Path.Combine(Path.GetTempPath(), "parquet_converter_strict_out_" + Guid.NewGuid().ToString("N")),
                    Strict: true,
                    FilesScanned: 4,
                    FilesConverted: 2,
                    FilesSkipped: 2,
                    FilesWithErrors: 0,
                    ManifestPath: Path.Combine(sourceDir, "..", "folder_conversion_manifest.json"),
                    InventoryCsvPath: null,
                    ReportPath: null)
            };
            var viewModel = new ParquetConverterViewModel(
                bridge,
                new FakeFolderPickerService(),
                new InMemorySettingsService())
            {
                SourceDirectory = sourceDir,
                StrictMode = true
            };

            await viewModel.RunConversionAsync();

            Assert.Equal("Strict mode blocked completion", viewModel.Status);
            Assert.Equal(4, viewModel.FilesScanned);
            Assert.Equal(2, viewModel.FilesConverted);
            Assert.Equal(2, viewModel.FilesSkipped);
            Assert.Contains("Strict mode blocked conversion", viewModel.ResultDetail);
            Assert.True(bridge.LastStrictMode);
        }
        finally
        {
            if (Directory.Exists(sourceDir))
            {
                Directory.Delete(sourceDir, recursive: true);
            }
        }
    }

    private sealed class RecordingEngineBridgeService : IEngineBridgeService
    {
        public FolderConversionResult Result { get; set; } = new(
            ExitCode: 0,
            Stdout: string.Empty,
            Stderr: string.Empty,
            SourceRoot: string.Empty,
            OutputDirectory: string.Empty,
            Strict: false,
            FilesScanned: null,
            FilesConverted: null,
            FilesSkipped: null,
            FilesWithErrors: null,
            ManifestPath: null,
            InventoryCsvPath: null,
            ReportPath: null);

        public string? LastSourceDirectory { get; private set; }
        public string? LastOutputDirectory { get; private set; }
        public bool LastStrictMode { get; private set; }

        public async IAsyncEnumerable<ProgressEvent> RunAsync(
            EngineRunRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<ProgressEvent> SimulatePolicyStreamAsync(
            PolicySimulationRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<EngineCommandResult> SimulatePolicyAsync(
            PolicySimulationRequest request,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<ConfigValidationResult> ValidateConfigAsync(
            string configPath,
            string? pythonPath,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> BuildLinkedAsync(
            string configPath,
            string outDirectory,
            string? pythonPath,
            bool includeRawGdelt,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> EvaluateAsync(
            string configPath,
            string outDirectory,
            string? pythonPath,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<EngineCommandResult> ImportOhlcvAsync(
            string sourceDirectory,
            string destinationDirectory,
            string? pythonPath,
            bool normalize = true,
            string? dateColumn = null,
            string? delimiter = null,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

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
            => throw new NotImplementedException();

        public Task<FolderConversionResult> ConvertFolderToParquetAsync(
            string sourceDirectory,
            string? outDirectory,
            string? pythonPath,
            bool strict = false,
            CancellationToken cancellationToken = default)
        {
            LastSourceDirectory = sourceDirectory;
            LastOutputDirectory = outDirectory;
            LastStrictMode = strict;
            return Task.FromResult(Result);
        }

        public Task<RunDiffResult> DiffRunsAsync(
            string runA,
            string runB,
            string? pythonPath,
            CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }

    private sealed class FakeFolderPickerService : IFolderPickerService
    {
        private readonly Queue<string?> _results;

        public FakeFolderPickerService(params string?[] results)
        {
            _results = new Queue<string?>(results);
        }

        public Task<string?> PickFolderAsync(
            string title,
            string? initialPath = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var value = _results.Count > 0 ? _results.Dequeue() : null;
            return Task.FromResult(value);
        }
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
