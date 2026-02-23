using Microsoft.Azure.Functions.Worker;

namespace SqlDataIntegrationFunctionTriggerApp;

/// <summary>
/// Durable entity that stores the most recent error message and timestamp for diagnostics.
/// </summary>
public class LastErrorEntity
{
    /// <summary>Most recent error message captured for the entity.</summary>
    public string LastError { get; set; }

    /// <summary>UTC timestamp when <see cref="LastError"/> was last updated.</summary>
    public DateTime LastErrorDate { get; set; }

    /// <summary>Sets the last error message and updates the timestamp to <see cref="DateTime.UtcNow"/>.</summary>
    public void Save(string lastError)
    {
        this.LastError = lastError;
        this.LastErrorDate = DateTime.UtcNow;
    }

    /// <summary>Azure Functions entry point that dispatches operations to <see cref="LastErrorEntity"/>.</summary>
    [Function(nameof(LastError))]
    public static Task LastErrorRun([EntityTrigger] TaskEntityDispatcher dispatcher)
        => dispatcher.DispatchAsync<LastErrorEntity>();
}

/// <summary>
/// Durable entity that stores a serialized representation of allowed columns configuration.
/// </summary>
public class AllowedColumnsEntity
{
    /// <summary>Serialized allowed columns configuration.</summary>
    public string AllowedColumns { get; set; }

    /// <summary>Persists the allowed columns configuration.</summary>
    public void Save(string allowedColumns)
    {
        this.AllowedColumns = allowedColumns;
    }

    /// <summary>Azure Functions entry point that dispatches operations to <see cref="AllowedColumnsEntity"/>.</summary>
    [Function(nameof(AllowedColumns))]
    public static Task AllowedColumnsRun([EntityTrigger] TaskEntityDispatcher dispatcher)
        => dispatcher.DispatchAsync<AllowedColumnsEntity>();
}