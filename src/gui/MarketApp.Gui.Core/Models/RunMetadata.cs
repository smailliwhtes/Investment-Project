namespace MarketApp.Gui.Core.Models;

public sealed record RunMetadata(string RunId, DateTime? TimestampUtc, string ConfigHash, int SymbolCount, int EligibleCount, string LastDate, int LagDays);
