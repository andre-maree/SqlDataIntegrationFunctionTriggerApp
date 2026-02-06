using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.DurableTask.Client;

namespace SqlDataIntegrationFunctionTriggerApp;

/// <summary>
/// SQL-triggered Functions entry points. Each function is bound to a specific SQL table.
/// Receives change batches from the Azure SQL trigger extension and delegates processing
/// to the ExecuteTriggerHelper extension method on FunctionContext.
/// </summary>
public class SqlTriggerFunctions
{
    private readonly IDataSyncAction _action;
    private const string Table_TrackingDemo = "dbo.TrackingDemo";
    private const string Table_TrackingDemo2 = "dbo.TrackingDemo2";

    public SqlTriggerFunctions(IDataSyncAction action)
    {
        _action = action;
    }

    /// <summary>
    /// Triggered when changes are committed to dbo.TrackingDemo.
    /// The SqlTrigger binding:
    /// - Uses "SqlConnectionString" app setting to connect.
    /// - Uses a lease name "lease_dbo.TrackingDemo" for checkpointing/redelivery.
    /// Changes are filtered to allowed columns and posted via the configured action.
    /// </summary>
    [Function(nameof(Table_TrackingDemo_Function))]
    public async Task Table_TrackingDemo_Function(
            [SqlTrigger(Table_TrackingDemo, "SqlConnectionString", "lease_" + Table_TrackingDemo)]
            IReadOnlyList<SqlChange<JsonObject>> changes,
            [DurableClient] DurableTaskClient client,
            FunctionContext context)
    {   
        // Delegate to helper: filter allowed columns, execute downstream action,
        // and coordinate durable retry/notification on failure.
        await ExecuteTriggerHelper.ExecuteChangeTrigger(context, changes, Table_TrackingDemo, client, _action, ["/post", Table_TrackingDemo]);
    }

    /// <summary>
    /// Triggered when changes are committed to dbo.TrackingDemo2.
    /// Binding semantics are the same as dbo.TrackingDemo, with a distinct lease.
    /// </summary>
    [Function(nameof(Table_TrackingDemo2_Function))]
    public async Task Table_TrackingDemo2_Function(
            [SqlTrigger(Table_TrackingDemo2, "SqlConnectionString", "lease_" + Table_TrackingDemo2)]
            IReadOnlyList<SqlChange<JsonObject>> changes,
            [DurableClient] DurableTaskClient client,
            FunctionContext context)
    {
        // Delegate to helper for filtering, action execution, and durable coordination.
        await ExecuteTriggerHelper.ExecuteChangeTrigger(context, changes, Table_TrackingDemo2, client, _action, ["/post", Table_TrackingDemo2]);
    }
}