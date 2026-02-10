using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.DependencyInjection;
using SqlDataIntegrationFunctionTriggerApp.Models;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace SqlDataIntegrationFunctionTriggerApp;

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
    public static async Task ExecuteChangeTrigger(
        FunctionContext context,
        IReadOnlyList<SqlChange<JsonObject>> changes,
        string table,
        DurableTaskClient client,
        IDataSyncAction action,
        params object[] parameters)
    {
        try
        {
            #region Allowed columns check

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
                ClientAllowedColumnsFunction.ParseColumns(out doClientAllowedColumnsCheck, out clientAllowedColumns, clientColumns);
            }

            // Load configuration-specified allowed columns from environment variables
            string? configColumns = Environment.GetEnvironmentVariable("AllowedColumns_" + table);

            if (!string.IsNullOrWhiteSpace(configColumns))
            {
                ClientAllowedColumnsFunction.ParseColumns(out doConfigAllowedColumnsCheck, out configAllowedColumns, configColumns);
            }

            // Build a single allowlist for O(1) membership checks
            HashSet<string>? allowList = null;

            if (doClientAllowedColumnsCheck || doConfigAllowedColumnsCheck)
            {
                allowList = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (doConfigAllowedColumnsCheck)
                {
                    for (int i = 0; i < configAllowedColumns.Length; i++)
                    {
                        allowList.Add(configAllowedColumns[i]);
                    }
                }

                if (doClientAllowedColumnsCheck)
                {
                    for (int i = 0; i < clientAllowedColumns.Length; i++)
                    {
                        allowList.Add(clientAllowedColumns[i]);
                    }
                }
            }

            // For each changed item, remove properties not present in allowed columns
            foreach (SqlChange<JsonObject> change in changes)
            {
                JsonObject item = change.Item;

                // Collect keys to remove to avoid repeated ElementAt/Remove (O(n^2))
                List<string>? keysToRemove = null;

                foreach (KeyValuePair<string, JsonNode?> prop in item)
                {
                    // If an allowlist exists and the prop is not allowed, mark it for removal
                    if (allowList != null && !allowList.Contains(prop.Key))
                    {
                        keysToRemove ??= new List<string>();
                        keysToRemove.Add(prop.Key);
                    }
                }

                if (keysToRemove != null)
                {
                    for (int i = 0; i < keysToRemove.Count; i++)
                    {
                        item.Remove(keysToRemove[i]);
                    }
                }
            }

            #endregion

            // Execute downstream action (e.g., HTTP post) with filtered changes
            await action.ExecuteAction(changes, parameters);
        }
        catch (Exception ex)
        {
            // Extract a concise error message for logging/state
            string error = string.IsNullOrWhiteSpace(ex.GetBaseException().Message) ? "No error information" : ex.GetBaseException().Message;

            // Non-retryable marker comes from HttpPostAction throwing with "retry=false"
            bool mustRetry = !error.StartsWith("retry=false");

            // Persist last error to a durable entity so callers/operators can inspect failures
            EntityInstanceId entityId = new("LastError", table);

            await client.Entities.SignalEntityAsync(entityId, "Save", input: error);

            if (mustRetry)
            {
                var settings = context.InstanceServices.GetService<Microsoft.Extensions.Options.IOptions<AppSettings>>()!.Value;

                RetryOrchestrationObject retryOrchestrationObject = new()
                {
                    IntervalMinutes = settings.DurableFunctionRetryIntervalMinutes,
                    SqlActivityObject = new SqlActivityObject
                    {
                        RetryTimeoutSpan = TimeSpan.FromHours(settings.TotalRetryTimeOutHours),
                        StartDate = DateTime.UtcNow
                    }
                };

                await RetryFunctions.StartRetryOrchestrator(table, client, retryOrchestrationObject);
            }
            else
            {
                await NotifyFunctions.StartNotifyOrchectrator(table, client, error);
            }

            // Trigger a retry by the SQLtrigger function
            throw;
        }
    }
}