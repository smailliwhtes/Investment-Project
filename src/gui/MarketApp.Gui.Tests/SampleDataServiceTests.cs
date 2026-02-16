using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class SampleDataServiceTests
{
    [Fact]
    public void Dashboard_IncludesLastDateAndLag()
    {
        var service = new SampleDataService();

        var dashboard = service.GetDashboard();

        Assert.NotNull(dashboard.LastRun.LastDateMax);
        Assert.True(dashboard.LastRun.WorstLagDays > 0);
        Assert.Contains(dashboard.TopSymbols, s => s.LastDate.Contains("2025"));
    }

    [Fact]
    public async Task RunViewModel_SimulatesProgress()
    {
        var orchestrator = new SimulatedRunOrchestrator();
        var vm = new RunViewModel(orchestrator);

        await Task.Run(() => vm.StartCommand.Execute(null));
        await Task.Delay(1800);

        Assert.True(vm.Progress > 0);
        Assert.NotEmpty(vm.ProgressEvents);
    }
}
