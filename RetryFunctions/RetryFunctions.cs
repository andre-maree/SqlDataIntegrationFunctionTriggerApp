using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp;

public class RetryFunctions
{
    /// <summary>
    /// Durable orchestration that waits for a configurable interval, then calls an activity
    /// to inspect SQL trigger lease state and decide whether to continue retrying.
    /// </summary>
    [Function(nameof(RetryOrchestration))]
    public static async Task RetryOrchestration(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger("RetryOrchestration");

        logger.LogCritical($"Orchestration is running for {context.InstanceId}");

        // Input: retry cadence. The value is minutes or seconds depending on the chosen TimeSpan (see toggle below).
        int intervalInMinutes = context.GetInput<int>();

        // Toggle between minutes and seconds for testing
        TimeSpan fireAt = new(0, intervalInMinutes, 0);
        //TimeSpan fireAt = new(0, 0, intervalInMinutes);

        // Non-blocking timer inside the orchestrator; execution resumes after fireAt elapses
        await context.CreateTimer(fireAt, default);

        logger.LogCritical($"Orchestration {context.InstanceId} is now calling the CheckSqlStatus activity");

        RetryPolicy retryPolicy = new(1000, TimeSpan.FromSeconds(15), backoffCoefficient: 1.125, maxRetryInterval: TimeSpan.FromMinutes(5), retryTimeout: TimeSpan.FromDays(2));

        TaskOptions options = new(retryPolicy);

        // Call activity that inspects SQL lease table/state and durable entities to decide next action
        bool continueProcessing = await context.CallActivityAsync<bool>(nameof(CheckSqlStatus), options);

        // true => continue retry loop; false => stop retrying
        if (continueProcessing)
        {
            logger.LogError($"Failure: CheckSqlStatus activity for orchestration {context.InstanceId} returned true and is retrying");
        }
        else
        {
            logger.LogWarning($"Success: CheckSqlStatus activity for orchestration {context.InstanceId} returned false and will exit");
        }

        if (continueProcessing)
        {
            // ContinueAsNew keeps the orchestration running with a fresh history to avoid unbounded growth.
            // Pass the same interval to the next generation.
            context.ContinueAsNew(intervalInMinutes);
        }
    }

    /// <summary>
    /// Activity that determines whether to keep retrying:
    /// - Reads durable entity RetryCount for the table and enforces MaxNumberOfRetries.
    /// - Inspects the SQL Functions lease table for the latest attempt count.
    /// - If attempt count indicates recent failure (e.g., 5), adjust to allow redelivery and update RetryCount.
    /// - Returns false when no further retries are needed (e.g., success or exceeded limits), true otherwise.
    /// </summary>
    [Function(nameof(CheckSqlStatus))]
    public async Task<bool> CheckSqlStatus([ActivityTrigger] FunctionContext executionContext, [DurableClient] DurableTaskClient client)
    {
        // Orchestration instance id is the table name; use it to scope entities and lease queries.
        string table = executionContext.BindingContext.BindingData["instanceid"].ToString();

        // Read current retry count entity; may be null on first failure.
        Microsoft.DurableTask.Client.Entities.EntityMetadata<RetryCountEntity>? retryCountEntity =
                await client.Entities.GetEntityAsync<RetryCountEntity>(new EntityInstanceId("RetryCount", table));

        int retryCountEntityInt;

        if (retryCountEntity != null)
        {
            retryCountEntityInt = retryCountEntity.State.RetryCount;

            // If we've exceeded the max retries, delete state and stop retrying.
            if (retryCountEntityInt > Convert.ToInt32(Environment.GetEnvironmentVariable("MaxNumberOfRetries")))
            {
                await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Delete");

                return false;
            }
        }
        else
        {
            retryCountEntityInt = 0;
        }

        // Connect to SQL and inspect the Azure Functions SQL trigger lease table for this table
        string? sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");

        using SqlConnection conn = new(sqlConnectionString);
        await conn.OpenAsync();

        // Get the highest attempt count recorded by the SQL trigger extension
        using SqlCommand cmd = new($"select max(_az_func_AttemptCount) from [az_func].[lease_{table}]", conn);
        object? result = await cmd.ExecuteScalarAsync();

        // No attempt count means nothing to retry; clear state and stop
        if (result == null || result == DBNull.Value)
        {
            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Delete");

            return false;
        }

        int sqlattemptCount = Convert.ToInt32(result);

        // < 1 indicates there is nothing pending for redelivery; clear state and stop
        if (sqlattemptCount < 1)
        {
            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Delete");

            return false;
        }

        // When attempt count reaches 5, nudge it back to 4 to allow the extension to retry again.
        // Also track and optionally notify when reaching the configured threshold.
        if (sqlattemptCount == 5)
        {
            string commandText = $"UPDATE [az_func].[lease_{table}] SET [_az_func_AttemptCount] = 4 WHERE [_az_func_AttemptCount] = 5;";
            cmd.CommandText = commandText;

            await cmd.ExecuteNonQueryAsync();

            // If we've hit the notification threshold, start a NotifyOrchestrator
            if (retryCountEntityInt == Convert.ToInt32(Environment.GetEnvironmentVariable("NotifyOnRetryCount")))
            {
                string notifyInstanceId = table + "_notify_" + Guid.NewGuid().ToString("N");

                await client.ScheduleNewOrchestrationInstanceAsync(
                    "NotifyOrchestrator",
                    options: new StartOrchestrationOptions(InstanceId: notifyInstanceId),
                    input: $"The action for table {table} has reached {retryCountEntityInt} retries.");
            }

            // Increment durable RetryCount and persist
            retryCountEntityInt++;
            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Save", input: retryCountEntityInt);
        }

        // Returning true keeps the orchestration alive and retrying; false ends it.
        return true;
    }
}
