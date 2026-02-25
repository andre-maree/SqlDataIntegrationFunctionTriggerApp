public sealed class AppSettings
{
    public int TotalRetryTimeOutHours { get; set; }
    public int RetryIntervalMinutesFirst { get; set; }
    public int RetryIntervalMinutesMax { get; set; }
    public string? SqlConnectionString { get; set; }
    public int NotifyOnRetryCount { get; set; }
}