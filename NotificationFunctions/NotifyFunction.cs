using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp;

public static class NotifyFunction
{
    [Function(nameof(NotifyOrchestrator))]
    public static async Task NotifyOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        string? input = context.GetInput<string>();

        RetryPolicy retryPolicy = new(firstRetryInterval: TimeSpan.FromSeconds(30), maxNumberOfAttempts: 100);

        TaskOptions options = new(retryPolicy);

        await context.CallActivityAsync<string>(nameof(Notify), input, options);

        await context.CreateTimer(context.CurrentUtcDateTime.AddHours(6), default);
    }

    [Function(nameof(Notify))]
    public static async Task Notify([ActivityTrigger] string message, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("Notify");

        logger.LogError($"Notification: {message}");
        //handle notification logic here, e.g., send email or call an API, send to queue, etc.
    }
}