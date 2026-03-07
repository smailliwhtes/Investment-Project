using System.Globalization;

namespace MarketApp.Gui.Core;

public sealed class QualityMetricsService : IQualityMetricsService
{
    public Task<RunQualitySnapshot?> LoadRunQualityAsync(
        RunSummary run,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(run.RunDirectory))
        {
            return Task.FromResult<RunQualitySnapshot?>(null);
        }

        var metricsPath = Path.Combine(run.RunDirectory, "eval", "eval_metrics.csv");
        if (!File.Exists(metricsPath))
        {
            return Task.FromResult<RunQualitySnapshot?>(null);
        }

        var rows = ReadCsvRows(metricsPath);
        if (rows.Count == 0)
        {
            return Task.FromResult<RunQualitySnapshot?>(null);
        }

        var grouped = rows
            .GroupBy(r => (GetValue(r, "model") ?? "unknown").Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(group => new BacktestMetricRow(
                Model: group.Key,
                Mse: Average(group, "mse"),
                Mae: Average(group, "mae"),
                Accuracy: Average(group, "accuracy"),
                F1: Average(group, "f1"),
                Splits: group.Count()))
            .OrderBy(r => r.Model, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (grouped.Length == 0)
        {
            return Task.FromResult<RunQualitySnapshot?>(null);
        }

        var evaluatedAt = File.GetLastWriteTimeUtc(metricsPath);
        return Task.FromResult<RunQualitySnapshot?>(new RunQualitySnapshot(run.RunId, evaluatedAt, grouped));
    }

    private static IReadOnlyList<Dictionary<string, string>> ReadCsvRows(string path)
    {
        var lines = File.ReadAllLines(path);
        if (lines.Length <= 1)
        {
            return Array.Empty<Dictionary<string, string>>();
        }

        var headers = SplitCsvLine(lines[0]);
        var rows = new List<Dictionary<string, string>>();

        foreach (var line in lines.Skip(1))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = SplitCsvLine(line);
            var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < headers.Count; i++)
            {
                row[headers[i]] = i < values.Count ? values[i] : string.Empty;
            }

            rows.Add(row);
        }

        return rows;
    }

    private static List<string> SplitCsvLine(string line)
    {
        var values = new List<string>();
        var current = new System.Text.StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var ch = line[i];
            if (ch == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }

                continue;
            }

            if (ch == ',' && !inQuotes)
            {
                values.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        values.Add(current.ToString());
        return values;
    }

    private static double? Average(IEnumerable<Dictionary<string, string>> rows, string key)
    {
        var values = rows
            .Select(r => ParseNullableDouble(GetValue(r, key)))
            .Where(v => v.HasValue)
            .Select(v => v!.Value)
            .ToArray();

        if (values.Length == 0)
        {
            return null;
        }

        return values.Average();
    }

    private static string? GetValue(Dictionary<string, string> row, string key)
    {
        return row.TryGetValue(key, out var value) ? value : null;
    }

    private static double? ParseNullableDouble(string? value)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return null;
    }
}
