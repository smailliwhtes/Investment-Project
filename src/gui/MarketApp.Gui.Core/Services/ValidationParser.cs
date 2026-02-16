using System.Text.Json;
using MarketApp.Gui.Core.Models;

namespace MarketApp.Gui.Core.Services;

public static class ValidationParser
{
    public static ConfigValidationResult Parse(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var valid = root.TryGetProperty("valid", out var v) && v.GetBoolean();
            var errors = new List<ConfigValidationError>();
            if (root.TryGetProperty("errors", out var arr) && arr.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in arr.EnumerateArray())
                {
                    errors.Add(new ConfigValidationError(
                        item.TryGetProperty("path", out var p) ? p.GetString() ?? string.Empty : string.Empty,
                        item.TryGetProperty("message", out var m) ? m.GetString() ?? string.Empty : string.Empty));
                }
            }
            return new ConfigValidationResult(valid, errors);
        }
        catch
        {
            return new ConfigValidationResult(false, new[] { new ConfigValidationError("$", "Invalid validation payload") });
        }
    }
}
