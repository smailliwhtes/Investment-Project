using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class AsyncRelayCommandTests
{
    [Fact]
    public async Task Execute_WhenDelegateThrows_ResetsCanExecuteState()
    {
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var command = new AsyncRelayCommand(async () =>
        {
            started.TrySetResult(true);
            await Task.Yield();
            throw new InvalidOperationException("boom");
        });

        command.Execute(null);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Allow async-void command to complete its finally block.
        await Task.Delay(50);

        Assert.True(command.CanExecute(null));
    }
}
