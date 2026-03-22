using MarketApp.Gui.Core;

namespace MarketApp.Gui.Tests;

public class EngineBridgeServiceTests
{
    [Fact]
    public void TryParseProgressEvent_ParsesContractJsonl()
    {
        var line = "{" +
                   "\"ts\":\"2026-01-31T12:00:00Z\"," +
                   "\"type\":\"artifact_emitted\"," +
                   "\"stage\":\"outputs\"," +
                   "\"pct\":1.0," +
                   "\"message\":\"artifact ready\"," +
                   "\"counters\":{\"done\":4,\"total\":4,\"units\":\"files\"}," +
                   "\"artifact\":{\"name\":\"scored.csv\",\"path\":\"scored.csv\",\"rows\":42,\"hash\":\"abc\"}," +
                   "\"error\":{\"code\":\"NONE\",\"detail\":\"\",\"traceback\":null}" +
                   "}";

        var parsed = EngineBridgeService.TryParseProgressEvent(line, out var evt);

        Assert.True(parsed);
        Assert.Equal("artifact_emitted", evt.Type);
        Assert.Equal("outputs", evt.Stage);
        Assert.Equal("scored.csv", evt.Artifact?.Name);
        Assert.Equal(42, evt.Artifact?.Rows);
        Assert.Equal(4, evt.Counters?.Done);
    }

    [Fact]
    public void TryParseProgressEvent_RejectsMalformedLine()
    {
        var parsed = EngineBridgeService.TryParseProgressEvent("not-json", out _);

        Assert.False(parsed);
    }

    [Fact]
    public void ResolvePythonPath_PrefersConfiguredPath()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "engine_bridge_py_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var pythonPath = Path.Combine(tempDir, "python.exe");
            File.WriteAllText(pythonPath, string.Empty);

            var resolved = EngineBridgeService.ResolvePythonPath(pythonPath, tempDir);

            Assert.Equal(Path.GetFullPath(pythonPath), resolved);
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
    public void ResolvePythonPath_UsesRepoVenvThenMarketAppVenv()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "engine_bridge_repo_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var repoVenv = Path.Combine(tempDir, ".venv", "Scripts");
            Directory.CreateDirectory(repoVenv);
            var repoPython = Path.Combine(repoVenv, "python.exe");
            File.WriteAllText(repoPython, string.Empty);

            var marketVenv = Path.Combine(tempDir, "market_app", ".venv", "Scripts");
            Directory.CreateDirectory(marketVenv);
            var marketPython = Path.Combine(marketVenv, "python.exe");
            File.WriteAllText(marketPython, string.Empty);

            var resolved = EngineBridgeService.ResolvePythonPath(null, tempDir);

            Assert.Equal(repoPython, resolved);

            File.Delete(repoPython);
            var fallback = EngineBridgeService.ResolvePythonPath(null, tempDir);
            Assert.Equal(marketPython, fallback);
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
    public void BuildRunArguments_IncludesOfflineWatchlistAndRawGdelt()
    {
        var request = new EngineRunRequest(
            ConfigPath: "config/config.yaml",
            OutDirectory: "outputs/runs/test",
            Offline: true,
            PythonPath: null,
            RunBuildLinked: false,
            RunEvaluate: false,
            IncludeRawGdelt: true,
            WatchlistPath: "watchlists/watchlist.csv");

        var args = EngineBridgeService.BuildRunArguments(request, request.OutDirectory);

        Assert.Contains("--offline", args);
        Assert.Contains("--watchlist", args);
        Assert.Contains("--include-raw-gdelt", args);
        Assert.Contains("--progress-jsonl", args);
    }

    [Fact]
    public void BuildPolicySimulationArguments_IncludesScenarioOfflineAndOutdir()
    {
        var request = new PolicySimulationRequest(
            ConfigPath: "config/config.yaml",
            ScenarioName: "tariff-shock",
            OutDirectory: "outputs/policy_simulations/test",
            Offline: true,
            PythonPath: null);

        var args = EngineBridgeService.BuildPolicySimulationArguments(request, Path.GetFullPath("."));

        Assert.Equal(new[] { "-m", "market_monitor.cli", "policy", "simulate" }, args.Take(4));
        Assert.Contains("--config", args);
        Assert.Contains("--scenario", args);
        Assert.Contains("tariff-shock", args);
        Assert.Contains("--outdir", args);
        Assert.Contains("--offline", args);
        Assert.Contains("--progress-jsonl", args);
    }

    [Fact]
    public void ResolveConfigPath_UsesMarketAppRelativePathWhenRepoRelativeMissing()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "engine_bridge_cfg_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var marketAppRoot = Path.Combine(tempDir, "market_app");
            var configDir = Path.Combine(marketAppRoot, "config");
            Directory.CreateDirectory(configDir);

            var expected = Path.Combine(configDir, "config.yaml");
            File.WriteAllText(expected, "app: market_monitor");

            var resolved = EngineBridgeService.ResolveConfigPath("config/config.yaml", marketAppRoot, tempDir);

            Assert.Equal(expected, resolved);
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
    public void ResolveEnginePath_UsesMarketAppRelativePathWhenRepoRelativeMissing()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "engine_bridge_path_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var marketAppRoot = Path.Combine(tempDir, "market_app");
            var watchlistDir = Path.Combine(marketAppRoot, "watchlists");
            Directory.CreateDirectory(watchlistDir);

            var expected = Path.Combine(watchlistDir, "watchlist.csv");
            File.WriteAllText(expected, "symbol\nAAPL");

            var resolved = EngineBridgeService.ResolveEnginePath("watchlists/watchlist.csv", marketAppRoot, tempDir);

            Assert.Equal(expected, resolved);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Theory]
    [InlineData(false, false, true, true)]
    [InlineData(true, false, true, false)]
    [InlineData(false, true, true, false)]
    [InlineData(false, false, false, false)]
    public void ShouldRunPostRunActions_RequiresCompletedSuccessfulRun(
        bool canceled,
        bool runHadError,
        bool runCompleted,
        bool expected)
    {
        var actual = EngineBridgeService.ShouldRunPostRunActions(canceled, runHadError, runCompleted);

        Assert.Equal(expected, actual);
    }
    [Fact]
    public void BuildStartInfo_SetsPythonUtf8Environment()
    {
        var info = EngineBridgeService.BuildStartInfo(
            "python",
            new[] { "-m", "market_monitor.cli", "validate-config" },
            Directory.GetCurrentDirectory());

        Assert.Equal("1", info.Environment["PYTHONUTF8"]);
        Assert.Equal("utf-8", info.Environment["PYTHONIOENCODING"]);
    }
}

