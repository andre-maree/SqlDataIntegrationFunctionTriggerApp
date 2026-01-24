using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;

namespace DataChangeTrackingFunctionApp;

public class ClientAllowedColumnsFunction
{
    /// <summary>
    /// Save allowed columns for a specific table
    /// </summary>
    /// <param name="table">sql table name</param>
    /// <param name="allowedColumns">comma separated list of allowed columns</param>
    [Function(nameof(SaveClientAllowedColumns))]
    public async Task<IActionResult> SaveClientAllowedColumns([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "saveClientAllowedColumns/{table}/{allowedColumns}")] HttpRequest req,
        [DurableClient] DurableTaskClient client,
        string table, string allowedColumns)
    {
        EntityInstanceId entityId = new("AllowedColumns", table.Replace("[", string.Empty).Replace("]", string.Empty));

        await client.Entities.SignalEntityAsync(entityId, "Save", input: allowedColumns);

        return new OkResult();
    }

    /// <summary>
    /// Get allowed columns for a specific table
    /// </summary>
    /// <param name="table">sql table name</param>
    [Function(nameof(GetClientAllowedColumns))]
    public async Task<IActionResult> GetClientAllowedColumns([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "getClientAllowedColumns/{table}")] HttpRequest req,
        [DurableClient] DurableTaskClient client,
        string table)
    {
        string? columns = await GetAllowedColumnsForTable(client, table);

        if (columns == null)
        {
            return new NoContentResult();
        }

        return new OkObjectResult(columns);
    }

    public static async Task<string?> GetAllowedColumnsForTable(DurableTaskClient client, string table)
    {
        table = table.Replace("[", string.Empty).Replace("]", string.Empty);

        EntityInstanceId entityId = new("AllowedColumns", table);

        Microsoft.DurableTask.Client.Entities.EntityMetadata<AllowedColumnsEntity>? t = await client.Entities.GetEntityAsync<AllowedColumnsEntity>(entityId);

        return t?.State.AllowedColumns;
    }
}