using MarketApp.Gui.Core;
using Microsoft.Maui.Storage;

namespace MarketApp.Gui.Services;

public class SecureSecretsStore : ISecretsStore
{
    public async Task SetAsync(string key, string value)
    {
        await SecureStorage.SetAsync(key, value);
    }

    public async Task<string?> GetAsync(string key)
    {
        try
        {
            return await SecureStorage.GetAsync(key);
        }
        catch (Exception ex) when (ex is InvalidOperationException or PlatformNotSupportedException)
        {
            System.Diagnostics.Debug.WriteLine($"SecureStorage unavailable: {ex.Message}");
            return null;
        }
    }

    public Task RemoveAsync(string key)
    {
        SecureStorage.Remove(key);
        return Task.CompletedTask;
    }
}
