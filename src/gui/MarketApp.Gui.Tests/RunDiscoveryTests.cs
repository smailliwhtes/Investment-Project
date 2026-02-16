using MarketApp.Gui.Core.Services;

namespace MarketApp.Gui.Tests;

public class RunDiscoveryTests
{
    [Fact]
    public void DiscoverRuns_ReturnsRunFolders()
    {
        var temp = Path.Combine(Path.GetTempPath(), "gui-runs-test-" + Guid.NewGuid());
        Directory.CreateDirectory(temp);
        var runDir = Path.Combine(temp, "20260101T000000Z");
        Directory.CreateDirectory(runDir);

        var runs = RunDiscoveryService.DiscoverRuns(temp);
        Assert.Single(runs);
        Assert.Equal("20260101T000000Z", runs[0].RunId);
    }
}
