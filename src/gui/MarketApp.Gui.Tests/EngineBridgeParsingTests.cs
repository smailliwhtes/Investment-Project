using MarketApp.Gui.Core.Services;

namespace MarketApp.Gui.Tests;

public class EngineBridgeParsingTests
{
    [Fact]
    public void TryParseProgress_ParsesJsonl()
    {
        var ok = EngineBridgeService.TryParseProgress("{\"ts\":\"2026-01-19T00:00:00Z\",\"stage\":\"score\",\"pct\":80,\"message\":\"working\"}", out var evt);
        Assert.True(ok);
        Assert.Equal("score", evt.Stage);
        Assert.Equal(80, evt.Percent);
    }

    [Fact]
    public void ValidationParser_ParsesErrorList()
    {
        var result = ValidationParser.Parse("{\"valid\":false,\"errors\":[{\"path\":\"gates.max_lag\",\"message\":\"must be >= 0\"}]}");
        Assert.False(result.IsValid);
        Assert.Single(result.Errors);
    }
}
