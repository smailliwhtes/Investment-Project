using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class ContractsTests
{
    [Fact]
    public void ForecastOverlayModel_StoresContractFields()
    {
        var model = new ForecastOverlayModel(
            TrainedUntil: new DateTime(2025, 1, 31),
            HorizonPoints: 3,
            YHat: new[] { 1.0, 2.0, 3.0 },
            Lo: new[] { 0.9, 1.9, 2.9 },
            Hi: new[] { 1.1, 2.1, 3.1 }
        );

        Assert.Equal(3, model.HorizonPoints);
        Assert.Equal(3, model.YHat.Count);
    }
}
