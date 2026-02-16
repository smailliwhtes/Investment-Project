namespace MarketApp.Gui.Core;

public class SettingsViewModel : ViewModelBase
{
    private readonly ISecretsStore _secretsStore;
    private string _configPath = "config/config.yaml";
    private string _pythonPath = "python";
    private string? _finnhub;
    private string? _twelveData;
    private string? _alphaVantage;
    private string _status = "Offline ready";

    public SettingsViewModel(ISecretsStore secretsStore)
    {
        _secretsStore = secretsStore;
        Title = "Settings";
        SaveSecretsCommand = new AsyncRelayCommand(SaveSecretsAsync);
    }

    public string ConfigPath
    {
        get => _configPath;
        set => SetProperty(ref _configPath, value);
    }

    public string PythonPath
    {
        get => _pythonPath;
        set => SetProperty(ref _pythonPath, value);
    }

    public string? FinnhubKey
    {
        get => _finnhub;
        set => SetProperty(ref _finnhub, value);
    }

    public string? TwelveDataKey
    {
        get => _twelveData;
        set => SetProperty(ref _twelveData, value);
    }

    public string? AlphaVantageKey
    {
        get => _alphaVantage;
        set => SetProperty(ref _alphaVantage, value);
    }

    public string StatusMessage
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public AsyncRelayCommand SaveSecretsCommand { get; }

    public async Task InitializeAsync()
    {
        FinnhubKey = await _secretsStore.GetAsync("FINNHUB_API_KEY").ConfigureAwait(false);
        TwelveDataKey = await _secretsStore.GetAsync("TWELVEDATA_API_KEY").ConfigureAwait(false);
        AlphaVantageKey = await _secretsStore.GetAsync("ALPHAVANTAGE_API_KEY").ConfigureAwait(false);
    }

    private async Task SaveSecretsAsync()
    {
        await _secretsStore.SetAsync("FINNHUB_API_KEY", FinnhubKey ?? string.Empty).ConfigureAwait(false);
        await _secretsStore.SetAsync("TWELVEDATA_API_KEY", TwelveDataKey ?? string.Empty).ConfigureAwait(false);
        await _secretsStore.SetAsync("ALPHAVANTAGE_API_KEY", AlphaVantageKey ?? string.Empty).ConfigureAwait(false);
        StatusMessage = "Secrets saved locally";
    }
}
