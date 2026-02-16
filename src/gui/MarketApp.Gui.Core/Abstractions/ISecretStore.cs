namespace MarketApp.Gui.Core.Abstractions;

public interface ISecretStore
{
    Task SetAsync(string key, string value);
    Task<string?> GetAsync(string key);
    Task RemoveAsync(string key);
}
