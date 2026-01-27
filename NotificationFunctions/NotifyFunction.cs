using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.Logging;
//using static System.Runtime.InteropServices.JavaScript.JSType;

namespace SqlDataIntegrationFunctionTriggerApp;

public static class NotifyFunction
{
    [Function(nameof(NotifyOrchestrator))]
    public static async Task NotifyOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        try
        {
            string? input = context.GetInput<string>();

            TaskOptions options = new(new RetryPolicy(firstRetryInterval: TimeSpan.FromSeconds(15),
                                                      maxNumberOfAttempts: 25,
                                                      maxRetryInterval: TimeSpan.FromSeconds(45),
                                                      backoffCoefficient: 1.1125));

            await context.CallActivityAsync(nameof(Notify), input, options);

            // Only the singleton instance with _code suffix sets a timer to prevent too many rapid notifications
            if (context.InstanceId.EndsWith("_code"))
            {
                await context.CreateTimer(context.CurrentUtcDateTime.AddHours(6), default);
            }
        }
        catch (Exception ex)
        {
            // Persist notify error to a durable entity so callers/operators can inspect failures
            EntityInstanceId entityId = new("LastError", context.InstanceId);

            await context.Entities.SignalEntityAsync(entityId, "Save", input: ex.GetBaseException().Message);
        }
    }

    [Function(nameof(Notify))]
    public static async Task Notify([ActivityTrigger] string message, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("Notify");

        logger.LogWarning($"Notification: {message}");

        await Task.CompletedTask;
        //handle notification logic here, e.g., send email or call an API, send to queue, etc.
    }
}