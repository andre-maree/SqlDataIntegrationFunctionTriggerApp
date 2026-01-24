using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;

namespace DataChangeTrackingFunctionApp;

/// <summary>
/// Helper with an extension method on FunctionContext that:
/// - Loads allowed columns (config + client-specified),
/// - Filters incoming SQL change payloads to the allowed columns,
/// - Executes the configured action,
/// - Captures errors and coordinates durable retry/notification flows.
/// </summary>
public static class ExecuteTriggerHelper
{
    /// <summary>
    /// Filters change items to allowed columns and executes the sync action.
    /// On failure:
    /// - Persists the error to the LastError durable entity,
    /// - If retryable, schedules (or reuses) the RetryOrchestration and rethrows,
    /// - If non-retryable, schedules a NotifyOrchestrator.
    /// </summary>
    public static async Task ExcuteChangeTrigger(this FunctionContext context, IReadOnlyList<SqlChange<JsonObject>> changes, string table, DurableTaskClient client, IDataSyncAction action, params object[] parameters)
    {
        try
        {
            // Durable Entity id used to read/write allowed columns per table
            EntityInstanceId entityId = new("AllowedColumns", table);

            bool doConfigAllowedColumnsCheck = false;
            bool doClientAllowedColumnsCheck = false;
            string[] clientAllowedColumns = null;
            string[] configAllowedColumns = null;

            // Load client-specified allowed columns from Durable Entity (if any)
            string? clientColumns = await ClientAllowedColumnsFunction.GetAllowedColumnsForTable(client, table);

            if (!string.IsNullOrWhiteSpace(clientColumns))
            {
                doClientAllowedColumnsCheck = true;
                clientAllowedColumns = clientColumns
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .ToArray();
            }

            // Load configuration-specified allowed columns from environment variables
            string? configColumns = Environment.GetEnvironmentVariable("AllowedColumns_" + table);

            if (!string.IsNullOrWhiteSpace(configColumns))
            {
                doConfigAllowedColumnsCheck = true;
                configAllowedColumns = configColumns
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .ToArray();
            }

            // For each changed item, remove properties not present in allowed columns
            foreach (SqlChange<JsonObject> change in changes)
            {
                JsonObject item = change.Item;

                for (int i = 0; i < item.Count;)
                {
                    KeyValuePair<string, JsonNode?> prop = item.ElementAt(i);

                    // Config-based allowlist check
                    if (doConfigAllowedColumnsCheck && !configAllowedColumns.Contains(prop.Key))
                    {
                        item.Remove(prop.Key);

                        continue;
                    }

                    // Client-based allowlist check
                    if (doClientAllowedColumnsCheck && !clientAllowedColumns.Contains(prop.Key))
                    {
                        item.Remove(prop.Key);
                        continue;
                    }

                    i++;
                }
            }

            // Execute downstream action (e.g., HTTP post) with filtered changes
            await action.ExecuteAction(changes, parameters);
        }
        catch (Exception ex)
        {
            // Extract a concise error message for logging/state
            string error = string.IsNullOrWhiteSpace(ex.GetBaseException().Message) ? "No error information" : ex.GetBaseException().Message;

            // Non-retryable marker comes from HttpPostAction throwing with "retry=false"
            bool mustNotRetry = error.StartsWith("retry=false");

            // Persist last error to a durable entity so callers/operators can inspect failures
            EntityInstanceId entityId = new("LastError", table);

            await client.Entities.SignalEntityAsync(entityId, "Save", input: error);

            if (!mustNotRetry)
            {
                // Ensure a single RetryOrchestration per table; start if not running
                OrchestrationMetadata? retrystatus = await client.GetInstanceAsync(table);

                if (retrystatus == null || !retrystatus.IsRunning)
                {
                    await client.ScheduleNewOrchestrationInstanceAsync(
                        "RetryOrchestration",
                        options: new StartOrchestrationOptions(InstanceId: table),
                        input: Convert.ToInt16(Environment.GetEnvironmentVariable("DurableFunctionRetryIntervalMinutes")));
                }

                // Rethrow so the SQL trigger does not advance its checkpoint and will redeliver on next poll
                throw;
            }

            // For non-retryable failures, schedule a notification orchestrator with a fixed id
            string notifyInstanceId = table + "_notify_received_a_nonretryable_httpstatuscode";

            OrchestrationMetadata? notifystatus = await client.GetInstanceAsync(notifyInstanceId);

            if (notifystatus == null || !notifystatus.IsRunning)
            {
                await client.ScheduleNewOrchestrationInstanceAsync(
                    "NotifyOrchestrator",
                    options: new StartOrchestrationOptions(InstanceId: notifyInstanceId),
                    input: $"The action for table {table} encountered a non-retryable HTTP status code: {error}");
            }
        }
    }
}