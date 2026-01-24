using Microsoft.Azure.Functions.Worker;

namespace DataChangeTrackingFunctionApp;

public class LastErrorEntity
{
    public string LastError { get; set; }
    public DateTime LastErrorDate { get; set; }

    public void Save(string lastError)
    {
        this.LastError = lastError;
        this.LastErrorDate = DateTime.UtcNow;
    }

    [Function(nameof(LastError))]
    public static Task LastErrorRun([EntityTrigger] TaskEntityDispatcher dispatcher)
        => dispatcher.DispatchAsync<LastErrorEntity>();
}

public class AllowedColumnsEntity
{
    public string AllowedColumns { get; set; }

    public void Save(string allowedColumns)
    {
        this.AllowedColumns = allowedColumns;
    }

    [Function(nameof(AllowedColumns))]
    public static Task AllowedColumnsRun([EntityTrigger] TaskEntityDispatcher dispatcher)
        => dispatcher.DispatchAsync<AllowedColumnsEntity>();
}

public class RetryCountEntity
{
    public int RetryCount { get; set; }

    public void Save(int retryCount)
    {
        this.RetryCount = retryCount;
    }

    [Function(nameof(RetryCount))]
    public static Task RetryCountRun([EntityTrigger] TaskEntityDispatcher dispatcher)
        => dispatcher.DispatchAsync<RetryCountEntity>();
}