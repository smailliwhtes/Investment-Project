using MarketApp.Gui.Core.Abstractions;

namespace MarketApp.Gui.Services;

public sealed class MauiSecretStore : ISecretStore
{
    public async Task SetAsync(string key, string value) => await SecureStorage.Default.SetAsync(key, value);
    public async Task<string?> GetAsync(string key) => await SecureStorage.Default.GetAsync(key);
    public Task RemoveAsync(string key)
    {
        SecureStorage.Default.Remove(key);
        return Task.CompletedTask;
    }
}
