using System.Threading;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.DependencyInjection;
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
    [Function(nameof(RetryOrchestrator))]
    public static async Task RetryOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger("RetryOrchestration");

        logger.LogGrey($"Orchestration is running for {context.InstanceId}");

        // Input: retry cadence. The value is minutes or seconds depending on the chosen TimeSpan (see toggle below).
        RetryOrchestrationObject retryOrchestrationObject = context.GetInput<RetryOrchestrationObject>();

        int retryInterval = retryOrchestrationObject.RetryIntervalMinutesFirst + retryOrchestrationObject.SqlActivityObject.RetryCount;

        // Toggle between minutes and seconds for testing
        TimeSpan fireAt = new(hours: 0, minutes: retryInterval > retryOrchestrationObject.RetryIntervalMinutesMax ? retryOrchestrationObject.RetryIntervalMinutesMax : retryInterval, seconds: 0);

        // This is used during testing to make the interval short
        //TimeSpan fireAt = new(hours: 0, 0, seconds: 10);

        // Non-blocking timer inside the orchestrator; execution resumes after fireAt elapses
        await context.CreateTimer(fireAt, default);

        logger.LogGrey($"Orchestration {context.InstanceId} is now calling the CheckSqlStatus activity");
        
        RetryPolicy retryPolicy = new(
            maxNumberOfAttempts: 999999,
            firstRetryInterval: TimeSpan.FromMinutes(1),
            backoffCoefficient: 1.125,
            maxRetryInterval: TimeSpan.FromMinutes(retryOrchestrationObject.RetryIntervalMinutesMax),
            retryTimeout: retryOrchestrationObject.SqlActivityObject.RetryTimeoutSpan);

        TaskOptions options = new(retry: retryPolicy);

        retryOrchestrationObject.SqlActivityObject.RetryCount++;

        // Call activity that inspects SQL lease table/state and durable entities to decide next action
        bool continueProcessing = await context.CallActivityAsync<bool>(nameof(CheckSqlStatus), retryOrchestrationObject.SqlActivityObject, options);

        // true => continue retry loop; false => exit eternal orchestration
        if (continueProcessing)
        {
            logger.LogOrange($"Orchestration {context.InstanceId} is retrying with continueProcessing = true");

            // ContinueAsNew keeps the orchestration running with a fresh history to avoid unbounded growth.
            // Pass the same interval and incremented retry count to the next generation.
            context.ContinueAsNew(retryOrchestrationObject);
        }
        else
        {
            logger.LogOrange($"Success: CheckSqlStatus activity for orchestration {context.InstanceId} returned false and will exit");
        }
    }

    /// <summary> 
    /// Attempts to start the per-table RetryOrchestration up to 5 times,
    /// waiting 2 seconds between attempts. If an instance with the given table
    /// instanceId is already running, it returns immediately without scheduling.
    /// </summary>
    public static async Task StartRetryOrchestrator(string table, DurableTaskClient client, RetryOrchestrationObject retryOrchestrationObject)
    {
        // Try up to 5 times with a 2-second delay between attempts
        for (int attempt = 1; attempt <= 5; attempt++)
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
                    nameof(RetryOrchestrator),
                    options: new StartOrchestrationOptions(InstanceId: table),
                    input: retryOrchestrationObject);
            }
            catch
            {
                // If we have remaining attempts, wait 2 seconds and retry
                if (attempt < 5)
                {
                    await Task.Delay(2000);

                    continue;
                }

                throw;
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
    public static async Task<bool> CheckSqlStatus(
        [ActivityTrigger] SqlActivityObject sqlActivityObject,
        FunctionContext executionContext,
        [DurableClient] DurableTaskClient client)
    {
        var settings = executionContext.InstanceServices.GetService<Microsoft.Extensions.Options.IOptions<AppSettings>>()!.Value;

        if (sqlActivityObject.StartDate.Add(sqlActivityObject.RetryTimeoutSpan) < DateTime.UtcNow)
        {
            return false;
        }

        // Orchestration instance id is the table name; use it to scope entities and lease queries.
        string table = executionContext.BindingContext.BindingData["instanceid"].ToString();

        using var conn = new SqlConnection(settings.SqlConnectionString);
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
        if (sqlActivityObject.RetryCount == settings.NotifyOnRetryCount)
        {
            await NotifyFunctions.StartNotifyOrchectrator(table, client, $"The action for table {table} has reached {sqlActivityObject.RetryCount} retries.", instanceIdPostfix: "NotifyOnRetryCount");
        }

        // Returning true keeps the orchestration alive and retrying; false ends it.
        return true;
    }

    #endregion
}
