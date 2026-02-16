using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MarketApp.Gui.Core.Abstractions;
using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Core.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private readonly IEngineBridge _engineBridge;

    [ObservableProperty] private int progressPercent;
    [ObservableProperty] private string progressStage = "idle";
    [ObservableProperty] private string progressMessage = string.Empty;
    [ObservableProperty] private List<string> symbols = new();

    public MainViewModel(IEngineBridge engineBridge)
    {
        _engineBridge = engineBridge;
    }

    [RelayCommand]
    private async Task RunAsync()
    {
        var progress = new Progress<EngineProgressEvent>(evt =>
        {
            ProgressPercent = evt.Percent;
            ProgressStage = evt.Stage;
            ProgressMessage = evt.Message;
        });

        await _engineBridge.RunAsync(
            new EngineRunRequest("python", "market_app/config/config.yaml", "market_app/outputs/runs/gui", true, true),
            progress,
            CancellationToken.None);
    }
}
