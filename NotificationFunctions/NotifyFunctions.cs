using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp;

public static class NotifyFunctions
{
    /// <summary>
    /// Orchestrator that sends a notification via the `Notify` activity with retries.
    /// If the `InstanceId` ends with "_code", it throttles by waiting 6 hours before completing
    /// to avoid rapid repeat notifications.
    /// </summary>
    [Function(nameof(NotifyOrchestrator))]
    public static async Task NotifyOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
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
                await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(360), default);
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

    /// <summary> 
    /// Attempts to start the per-table NotifyOrchestrator up to 3 times,
    /// waiting 2 seconds between attempts. If an instance with the given
    /// instanceId is already running, it returns immediately without scheduling.
    /// <summary>
    public static async Task StartNotifyOrchectration(string table, DurableTaskClient client, string error, string instanceIdPostfix = "received_a_nonretryable_code")
    {
        string notifyInstanceId = $"{table}_notify_{instanceIdPostfix}";

        // Try up to 3 times with a 2-second delay between attempts
        for (int attempt = 0; attempt < 3; attempt++)
        {
            OrchestrationMetadata? notifyStatus = await client.GetInstanceAsync(notifyInstanceId);

            // If already running, nothing to start
            if (notifyStatus != null && notifyStatus.IsRunning)
            {
                return;
            }

            try
            {
                await client.ScheduleNewOrchestrationInstanceAsync(
                    "NotifyOrchestrator",
                    options: new StartOrchestrationOptions(InstanceId: notifyInstanceId),
                    input: error);

                return; // scheduled successfully
            }
            catch
            {
                // If we have remaining attempts, wait 2 seconds and retry
                if (attempt < 2)
                {
                    await Task.Delay(2000);

                    continue;
                }
            }
        }
    }
}