using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using SqlDataIntegrationFunctionTriggerApp.Models;

namespace SqlDataIntegrationFunctionTriggerApp;

public static class RetryFunctions
{
    #region Orchestration

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
        RetryObject retryObject = context.GetInput<RetryObject>();

        // Toggle between minutes and seconds for testing
        TimeSpan fireAt = new(hours: 0, minutes: retryObject.IntervalMinutes, seconds: 0);
        //TimeSpan fireAt = new(hours: 0, minutes: 0, seconds: retryObject.IntervalMinutes);


        // Non-blocking timer inside the orchestrator; execution resumes after fireAt elapses
        await context.CreateTimer(fireAt, default);

        logger.LogCritical($"Orchestration {context.InstanceId} is now calling the CheckSqlStatus activity");

        RetryPolicy retryPolicy = new(
            -1,
            TimeSpan.FromSeconds(15),
            backoffCoefficient: 1.125,
            maxRetryInterval: TimeSpan.FromMinutes(10),
            retryTimeout: TimeSpan.FromDays(7));

        TaskOptions options = new(retryPolicy);

        retryObject.RetryCount++;

        // Call activity that inspects SQL lease table/state and durable entities to decide next action
        bool continueProcessing = await context.CallActivityAsync<bool>(nameof(CheckSqlStatus), retryObject.RetryCount, options);

        // true => continue retry loop; false => exit eternal orchestration
        if (continueProcessing)
        {
            logger.LogWarning($"Orchestration {context.InstanceId} is retying with continueProcessing = true");

            // ContinueAsNew keeps the orchestration running with a fresh history to avoid unbounded growth.
            // Pass the same interval to the next generation.
            context.ContinueAsNew(retryObject);
        }
        else
        {
            logger.LogWarning($"Success: CheckSqlStatus activity for orchestration {context.InstanceId} returned false and will exit");
        }
    }

    /// <summary> 
    /// Attempts to start the per-table RetryOrchestration up to 3 times,
    /// waiting 2 seconds between attempts. If an instance with the given table
    /// instanceId is already running, it returns immediately without scheduling.
    /// </summary>
    public static async Task StartRetryOrchectration(string table, DurableTaskClient client)
    {
        // Try up to 3 times with a 2-second delay between attempts
        for (int attempt = 0; attempt < 3; attempt++)
        {
            OrchestrationMetadata? retrystatus = await client.GetInstanceAsync(table);

            // If already running, nothing to start
            if (retrystatus != null && retrystatus.IsRunning)
            {
                return;
            }

            try
            {
                await client.ScheduleNewOrchestrationInstanceAsync(
                    "RetryOrchestration",
                    options: new StartOrchestrationOptions(InstanceId: table),
                    input: new RetryObject()
                    {
                        IntervalMinutes = Convert.ToInt16(Environment.GetEnvironmentVariable("DurableFunctionRetryIntervalMinutes"))
                    });
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

    #endregion

    #region SQL check activity

    /// <summary>
    /// Activity that determines whether to keep retrying:
    /// - Reads durable entity RetryCount for the table and enforces MaxNumberOfRetries.
    /// - Inspects the SQL Functions lease table for the latest attempt count.
    /// - If attempt count indicates recent failure (e.g., 5), adjust to allow redelivery and update RetryCount.
    /// - Returns false when no further retries are needed (e.g., success or exceeded limits), true otherwise.
    /// </summary>
    [Function(nameof(CheckSqlStatus))]
    public static async Task<bool> CheckSqlStatus([ActivityTrigger] int retryCount, FunctionContext executionContext, [DurableClient] DurableTaskClient client)
    {
        // Orchestration instance id is the table name; use it to scope entities and lease queries.
        string table = executionContext.BindingContext.BindingData["instanceid"].ToString();

        // If we've exceeded the max retries, delete state and stop retrying.
        if (retryCount > Convert.ToInt32(Environment.GetEnvironmentVariable("MaxNumberOfRetries")))
        {
            return false;
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
            return false;
        }

        int sqlattemptCount = Convert.ToInt32(result);

        // < 1 indicates there is nothing pending for redelivery; clear state and stop
        if (sqlattemptCount < 1)
        {
            return false;
        }

        // When attempt count reaches 5, nudge it back to 4 to allow the extension to retry again.
        // Also track and optionally notify when reaching the configured threshold.
        if (sqlattemptCount == 5)
        {
            string commandText = $"UPDATE [az_func].[lease_{table}] SET [_az_func_AttemptCount] = 4 WHERE [_az_func_AttemptCount] = 5;";
            cmd.CommandText = commandText;

            await cmd.ExecuteNonQueryAsync();
        }

        // If we've hit the notification threshold, start a NotifyOrchestrator
        if (retryCount == Convert.ToInt32(Environment.GetEnvironmentVariable("NotifyOnRetryCount")))
        {            
            await NotifyFunctions.StartNotifyOrchectration(table, client, error);
        }

        // Returning true keeps the orchestration alive and retrying; false ends it.
        return true;
    }

    #endregion
}
