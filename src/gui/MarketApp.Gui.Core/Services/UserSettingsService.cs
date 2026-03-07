using System;

namespace MarketApp.Gui.Core;

public sealed class UserSettingsService : IUserSettingsService
{
    private const string PythonPathKey = "MARKETAPP_PYTHON_PATH";

    public string? GetPythonPath()
    {
        return Environment.GetEnvironmentVariable(PythonPathKey, EnvironmentVariableTarget.User)
            ?? Environment.GetEnvironmentVariable(PythonPathKey);
    }

    public void SetPythonPath(string? pythonPath)
    {
        if (string.IsNullOrWhiteSpace(pythonPath))
        {
            Environment.SetEnvironmentVariable(PythonPathKey, null, EnvironmentVariableTarget.User);
            Environment.SetEnvironmentVariable(PythonPathKey, null);
            return;
        }

        var normalized = pythonPath.Trim();
        Environment.SetEnvironmentVariable(PythonPathKey, normalized, EnvironmentVariableTarget.User);
        Environment.SetEnvironmentVariable(PythonPathKey, normalized);
    }
}
