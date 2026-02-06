public sealed class AppSettings
{
    public int DurableFunctionRetryIntervalMinutes { get; set; }
    public int MaxNumberOfRetries { get; set; }
    public int NotifyOnRetryCount { get; set; }
    public string? SqlConnectionString { get; set; }
}