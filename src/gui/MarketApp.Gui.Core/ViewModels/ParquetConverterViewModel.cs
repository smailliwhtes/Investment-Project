namespace MarketApp.Gui.Core;

public sealed class ParquetConverterViewModel : ViewModelBase
{
    private readonly IEngineBridgeService _engineBridge;
    private readonly IFolderPickerService _folderPicker;
    private readonly IUserSettingsService _settings;
    private CancellationTokenSource? _cts;
    private string _sourceDirectory = string.Empty;
    private string _outputDirectory = string.Empty;
    private string _pythonPath;
    private bool _strictMode;
    private string _status = "Choose a source folder to start a standalone Parquet conversion.";
    private string _resultDetail = "Supported files include CSV, TSV, PSV, TXT (tabular), JSON/JSONL, and existing Parquet.";
    private bool _isRunning;
    private int? _filesScanned;
    private int? _filesConverted;
    private int? _filesSkipped;
    private int? _filesWithErrors;
    private string? _manifestPath;
    private string? _inventoryCsvPath;
    private string? _reportPath;

    public ParquetConverterViewModel(
        IEngineBridgeService engineBridge,
        IFolderPickerService folderPicker,
        IUserSettingsService settings)
    {
        _engineBridge = engineBridge;
        _folderPicker = folderPicker;
        _settings = settings;
        _pythonPath = _settings.GetPythonPath() ?? "python";

        Title = "Folder To Parquet";
        BrowseSourceFolderCommand = new AsyncRelayCommand(BrowseSourceFolderAsync, () => !IsRunning);
        BrowseOutputFolderCommand = new AsyncRelayCommand(BrowseOutputFolderAsync, () => !IsRunning);
        StartConversionCommand = new AsyncRelayCommand(RunConversionAsync, () => !IsRunning && CanStartConversion());
        CancelCommand = new RelayCommand(CancelRun, () => IsRunning);
    }

    public string SourceDirectory
    {
        get => _sourceDirectory;
        set
        {
            if (SetProperty(ref _sourceDirectory, value))
            {
                OnPropertyChanged(nameof(DefaultOutputDirectoryPreview));
                OnPropertyChanged(nameof(OutputDirectoryHint));
                StartConversionCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public string OutputDirectory
    {
        get => _outputDirectory;
        set
        {
            if (SetProperty(ref _outputDirectory, value))
            {
                OnPropertyChanged(nameof(OutputDirectoryHint));
            }
        }
    }

    public string PythonPath
    {
        get => _pythonPath;
        set
        {
            if (SetProperty(ref _pythonPath, value))
            {
                _settings.SetPythonPath(value);
            }
        }
    }

    public bool StrictMode
    {
        get => _strictMode;
        set => SetProperty(ref _strictMode, value);
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string ResultDetail
    {
        get => _resultDetail;
        set => SetProperty(ref _resultDetail, value);
    }

    public bool IsRunning
    {
        get => _isRunning;
        set
        {
            if (SetProperty(ref _isRunning, value))
            {
                BrowseSourceFolderCommand.RaiseCanExecuteChanged();
                BrowseOutputFolderCommand.RaiseCanExecuteChanged();
                StartConversionCommand.RaiseCanExecuteChanged();
                CancelCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public int? FilesScanned
    {
        get => _filesScanned;
        private set
        {
            if (SetProperty(ref _filesScanned, value))
            {
                OnPropertyChanged(nameof(HasSummary));
            }
        }
    }

    public int? FilesConverted
    {
        get => _filesConverted;
        private set => SetProperty(ref _filesConverted, value);
    }

    public int? FilesSkipped
    {
        get => _filesSkipped;
        private set => SetProperty(ref _filesSkipped, value);
    }

    public int? FilesWithErrors
    {
        get => _filesWithErrors;
        private set => SetProperty(ref _filesWithErrors, value);
    }

    public string? ManifestPath
    {
        get => _manifestPath;
        private set
        {
            if (SetProperty(ref _manifestPath, value))
            {
                OnPropertyChanged(nameof(HasManifestPath));
            }
        }
    }

    public string? InventoryCsvPath
    {
        get => _inventoryCsvPath;
        private set
        {
            if (SetProperty(ref _inventoryCsvPath, value))
            {
                OnPropertyChanged(nameof(HasInventoryCsvPath));
            }
        }
    }

    public string? ReportPath
    {
        get => _reportPath;
        private set
        {
            if (SetProperty(ref _reportPath, value))
            {
                OnPropertyChanged(nameof(HasReportPath));
            }
        }
    }

    public string DefaultOutputDirectoryPreview => TryBuildDefaultOutputDirectory(SourceDirectory) ?? string.Empty;

    public string OutputDirectoryHint =>
        string.IsNullOrWhiteSpace(OutputDirectory) && !string.IsNullOrWhiteSpace(DefaultOutputDirectoryPreview)
            ? $"Leave output blank to write into {DefaultOutputDirectoryPreview}"
            : "Choose an output folder outside the source tree if you want to override the default.";

    public bool HasSummary => FilesScanned.HasValue;
    public bool HasManifestPath => !string.IsNullOrWhiteSpace(ManifestPath);
    public bool HasInventoryCsvPath => !string.IsNullOrWhiteSpace(InventoryCsvPath);
    public bool HasReportPath => !string.IsNullOrWhiteSpace(ReportPath);

    public AsyncRelayCommand BrowseSourceFolderCommand { get; }
    public AsyncRelayCommand BrowseOutputFolderCommand { get; }
    public AsyncRelayCommand StartConversionCommand { get; }
    public RelayCommand CancelCommand { get; }

    internal async Task BrowseSourceFolderAsync()
    {
        var selected = await _folderPicker.PickFolderAsync(
            "Choose the folder to convert",
            string.IsNullOrWhiteSpace(SourceDirectory) ? null : SourceDirectory);
        if (string.IsNullOrWhiteSpace(selected))
        {
            return;
        }

        SourceDirectory = selected;
        Status = "Source folder selected";
        ResultDetail = string.IsNullOrWhiteSpace(OutputDirectory)
            ? $"The converter will write a sibling Parquet tree into {DefaultOutputDirectoryPreview} unless you choose another output folder."
            : "Ready to run the conversion with the selected source folder.";
    }

    internal async Task BrowseOutputFolderAsync()
    {
        var selected = await _folderPicker.PickFolderAsync(
            "Choose the Parquet output folder",
            string.IsNullOrWhiteSpace(OutputDirectory) ? DefaultOutputDirectoryPreview : OutputDirectory);
        if (string.IsNullOrWhiteSpace(selected))
        {
            return;
        }

        OutputDirectory = selected;
        Status = "Output folder selected";
        ResultDetail = "The converted Parquet tree will be written into the selected output folder.";
    }

    internal async Task RunConversionAsync()
    {
        if (!CanStartConversion())
        {
            Status = "Source folder required";
            ResultDetail = "Choose an existing source folder before starting the conversion.";
            return;
        }

        var resolvedSource = Path.GetFullPath(SourceDirectory);
        if (!Directory.Exists(resolvedSource))
        {
            Status = "Source folder not found";
            ResultDetail = $"The selected source folder does not exist: {resolvedSource}";
            return;
        }

        _cts?.Dispose();
        _cts = new CancellationTokenSource();
        ResetSummary();
        IsRunning = true;
        Status = "Converting folder to Parquet";
        ResultDetail = "The engine is scanning supported files, converting them, and writing a manifest.";

        try
        {
            var result = await _engineBridge.ConvertFolderToParquetAsync(
                sourceDirectory: resolvedSource,
                outDirectory: string.IsNullOrWhiteSpace(OutputDirectory) ? null : OutputDirectory,
                pythonPath: string.IsNullOrWhiteSpace(PythonPath) ? null : PythonPath,
                strict: StrictMode,
                cancellationToken: _cts.Token);

            ApplyResult(result);
        }
        catch (OperationCanceledException)
        {
            Status = "Canceled";
            ResultDetail = "The folder conversion was canceled before it finished.";
        }
        catch (Exception ex)
        {
            Status = "Conversion failed";
            ResultDetail = ex.Message;
        }
        finally
        {
            IsRunning = false;
            _cts?.Dispose();
            _cts = null;
        }
    }

    private void ApplyResult(FolderConversionResult result)
    {
        FilesScanned = result.FilesScanned;
        FilesConverted = result.FilesConverted;
        FilesSkipped = result.FilesSkipped;
        FilesWithErrors = result.FilesWithErrors;
        ManifestPath = result.ManifestPath;
        InventoryCsvPath = result.InventoryCsvPath;
        ReportPath = result.ReportPath;

        ResultDetail = BuildResultDetail(result);
        Status = result.ExitCode switch
        {
            0 => "Conversion complete",
            130 => "Canceled",
            _ when result.Strict && (result.FilesSkipped ?? 0) > 0 && (result.FilesWithErrors ?? 0) == 0
                => "Strict mode blocked completion",
            _ => "Conversion finished with issues",
        };
    }

    private void ResetSummary()
    {
        FilesScanned = null;
        FilesConverted = null;
        FilesSkipped = null;
        FilesWithErrors = null;
        ManifestPath = null;
        InventoryCsvPath = null;
        ReportPath = null;
    }

    private bool CanStartConversion()
    {
        return !string.IsNullOrWhiteSpace(SourceDirectory);
    }

    private void CancelRun()
    {
        _cts?.Cancel();
    }

    private static string BuildResultDetail(FolderConversionResult result)
    {
        var lines = new List<string>();
        if (result.FilesScanned.HasValue)
        {
            lines.Add(
                $"Scanned {result.FilesScanned.Value} file(s): converted {result.FilesConverted ?? 0}, skipped {result.FilesSkipped ?? 0}, errors {result.FilesWithErrors ?? 0}.");
        }

        lines.Add($"Output folder: {result.OutputDirectory}");

        if (!string.IsNullOrWhiteSpace(result.Stderr))
        {
            lines.Add(result.Stderr.Trim());
        }
        else if (result.ExitCode != 0 && !string.IsNullOrWhiteSpace(result.Stdout))
        {
            lines.Add(result.Stdout.Trim());
        }
        else if (result.ExitCode == 0)
        {
            lines.Add("Review the manifest or report paths below if you need a complete inventory of converted files.");
        }

        return string.Join(Environment.NewLine, lines.Where(line => !string.IsNullOrWhiteSpace(line)));
    }

    private static string? TryBuildDefaultOutputDirectory(string sourceDirectory)
    {
        if (string.IsNullOrWhiteSpace(sourceDirectory))
        {
            return null;
        }

        try
        {
            var sourceInfo = new DirectoryInfo(Path.GetFullPath(sourceDirectory));
            var parentDirectory = sourceInfo.Parent?.FullName ?? sourceInfo.FullName;
            var folderName = string.IsNullOrWhiteSpace(sourceInfo.Name)
                ? "_parquet"
                : $"{sourceInfo.Name}_parquet";
            return Path.GetFullPath(Path.Combine(parentDirectory, folderName));
        }
        catch
        {
            return null;
        }
    }
}
