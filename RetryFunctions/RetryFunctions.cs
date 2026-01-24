using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.Logging;

namespace DataChangeTrackingFunctionApp;

public class RetryFunctions
{
    [Function(nameof(RetryOrchestration))]
    public static async Task RetryOrchestration(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger("RetryOrchestration");

        logger.LogCritical($"Orchestration is running for {context.InstanceId}");

        int intervalInMinutes = context.GetInput<int>();

        // Uncomment to toggle these 2 lines below to switch between minutes and seconds for testing
        TimeSpan fireAt = new(0, intervalInMinutes, 0);
        //TimeSpan fireAt = new(0, 0, intervalInMinutes);

        await context.CreateTimer(fireAt, default);

        logger.LogCritical($"Orchestration {context.InstanceId} is now calling the CheckSqlStatus activity");

        RetryPolicy retryPolicy = new(1000, TimeSpan.FromSeconds(15), backoffCoefficient: 1.125, maxRetryInterval: TimeSpan.FromMinutes(5), retryTimeout: TimeSpan.FromDays(2));
        TaskOptions options = new(retryPolicy);

        bool continueProcessing = await context.CallActivityAsync<bool>(nameof(CheckSqlStatus), options);

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
            // Restart this orchestration to keep it running without unbounded history
            context.ContinueAsNew(intervalInMinutes);
        }
    }

    [Function(nameof(CheckSqlStatus))]
    public async Task<bool> CheckSqlStatus([ActivityTrigger] FunctionContext executionContext, [DurableClient] DurableTaskClient client)
    {
        string table = executionContext.BindingContext.BindingData["instanceid"].ToString();

        Microsoft.DurableTask.Client.Entities.EntityMetadata<RetryCountEntity>? retryCountEntity =
                await client.Entities.GetEntityAsync<RetryCountEntity>(new EntityInstanceId("RetryCount", table));

        int retryCountEntityInt;

        if (retryCountEntity != null)
        {
            retryCountEntityInt = retryCountEntity.State.RetryCount;

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

        string? sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");

        using SqlConnection conn = new(sqlConnectionString);

        await conn.OpenAsync();

        using SqlCommand cmd = new($"select max(_az_func_AttemptCount) from [az_func].[lease_{table}]", conn);

        object? result = await cmd.ExecuteScalarAsync();

        if (result == null || result == DBNull.Value)
        {
            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Delete");

            return false;
        }

        int sqlattemptCount = Convert.ToInt32(result);

        if (sqlattemptCount < 1)
        {
            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Delete");

            return false;
        }

        if (sqlattemptCount == 5)
        {
            string commandText = $"UPDATE [az_func].[lease_{table}] SET [_az_func_AttemptCount] = 4 WHERE [_az_func_AttemptCount] = 5;";

            cmd.CommandText = commandText;

            await cmd.ExecuteNonQueryAsync();

            if (retryCountEntityInt == Convert.ToInt32(Environment.GetEnvironmentVariable("NotifyOnRetryCount")))
            {
                string notifyInstanceId = table + "_notify_" + Guid.NewGuid().ToString("N");

                await client.ScheduleNewOrchestrationInstanceAsync(
                "NotifyOrchestrator",
                options: new StartOrchestrationOptions(InstanceId: notifyInstanceId),
                input: $"The action for table {table} has reached {retryCountEntityInt} retries.");
            }

            retryCountEntityInt++;

            await client.Entities.SignalEntityAsync(new EntityInstanceId("RetryCount", table), "Save", input: retryCountEntityInt);
        }

        return true;
    }
}
