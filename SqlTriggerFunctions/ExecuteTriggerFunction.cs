using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;

namespace DataChangeTrackingFunctionApp;

public class ExecuteTriggerFunction
{
    public static async Task ExcuteChangeTrigger(IReadOnlyList<SqlChange<JsonObject>> changes, string table, DurableTaskClient client, IDataSyncAction action, FunctionContext context, params object[] parameters)
    {
        try
        {
            EntityInstanceId entityId = new("AllowedColumns", table);

            bool doConfigAllowedColumnsCheck = false;
            bool doClientAllowedColumnsCheck = false;
            string[] clientAllowedColumns = null;
            string[] configAllowedColumns = null;

            string? clientColumns = await ClientAllowedColumnsFunction.GetAllowedColumnsForTable(client, table);

            if (!string.IsNullOrWhiteSpace(clientColumns))
            {
                doClientAllowedColumnsCheck = true;
                clientAllowedColumns = clientColumns
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .ToArray();
            }

            string? configColumns = Environment.GetEnvironmentVariable("AllowedColumns_" + table);

            if (!string.IsNullOrWhiteSpace(configColumns))
            {
                doConfigAllowedColumnsCheck = true;
                configAllowedColumns = configColumns
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .ToArray();
            }

            foreach (SqlChange<JsonObject> change in changes)
            {
                JsonObject item = change.Item;

                for (int i = 0; i < item.Count;)
                {
                    KeyValuePair<string, JsonNode?> prop = item.ElementAt(i);

                    if (doConfigAllowedColumnsCheck && !configAllowedColumns.Contains(prop.Key))
                    {
                        item.Remove(prop.Key);

                        continue;
                    }

                    if (doClientAllowedColumnsCheck && !clientAllowedColumns.Contains(prop.Key))
                    {
                        item.Remove(prop.Key);

                        continue;
                    }

                    i++;
                }
            }

            await action.ExecuteAction(changes, parameters);
        }
        catch (Exception ex)
        {
            string error = string.IsNullOrWhiteSpace(ex.GetBaseException().Message) ? "No error information" : ex.GetBaseException().Message;

            bool mustNotRetry = error.StartsWith("retry=false");

            EntityInstanceId entityId = new("LastError", table);

            await client.Entities.SignalEntityAsync(entityId, "Save", input: error);

            if (!mustNotRetry)
            {
                OrchestrationMetadata? retrystatus = await client.GetInstanceAsync(table);

                if (retrystatus == null || !retrystatus.IsRunning)
                {
                    await client.ScheduleNewOrchestrationInstanceAsync("RetryOrchestration",
                        options: new StartOrchestrationOptions(InstanceId: table),
                        input: Convert.ToInt16(Environment.GetEnvironmentVariable("DurableFunctionRetryIntervalMinutes")));
                }

                throw;
            }

            string notifyInstanceId = table + "_notify_" + Guid.NewGuid().ToString("N");

            await client.ScheduleNewOrchestrationInstanceAsync(
                "NotifyOrchestrator",
                options: new StartOrchestrationOptions(InstanceId: notifyInstanceId),
                input: $"The action for table {table} encountered a non-retryable HTTP status code: {error}");

        }
    }
}