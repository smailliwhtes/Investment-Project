using System.Runtime.CompilerServices;

namespace MarketApp.Gui.Core;

public class SimulatedRunOrchestrator
{
    public async IAsyncEnumerable<ProgressEvent> RunAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var steps = new[]
        {
            new ProgressEvent("stage_start", "prepare", "Preparing inputs", 0.05, now),
            new ProgressEvent("stage_progress", "load_data", "Loading OHLCV cache", 0.2, now.AddSeconds(1)),
            new ProgressEvent("stage_progress", "score", "Scoring symbols", 0.45, now.AddSeconds(2)),
            new ProgressEvent("stage_progress", "score", "Scoring symbols", 0.7, now.AddSeconds(3)),
            new ProgressEvent("artifact_emitted", "write", "Wrote scored.csv", 0.85, now.AddSeconds(4)),
            new ProgressEvent("stage_end", "complete", "Run finished", 1.0, now.AddSeconds(5))
        };

        foreach (var step in steps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(250, cancellationToken).ConfigureAwait(false);
            yield return step;
        }
    }
}
