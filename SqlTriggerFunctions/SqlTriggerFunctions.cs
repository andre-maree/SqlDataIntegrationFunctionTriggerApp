using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.DurableTask.Client;

namespace DataChangeTrackingFunctionApp;

public class SqlTriggerFunctions
{
    private readonly IDataSyncAction _action;
    private const string Table_TrackingDemo = "dbo.TrackingDemo";
    private const string Table_TrackingDemo2 = "dbo.TrackingDemo2";

    public SqlTriggerFunctions(IDataSyncAction action)
    {
        _action = action;
    }

    [Function(nameof(Table_TrackingDemo_Function))]
    public async Task Table_TrackingDemo_Function(
            [SqlTrigger(Table_TrackingDemo, "SqlConnectionString", "lease_" + Table_TrackingDemo)]
            IReadOnlyList<SqlChange<JsonObject>> changes,
            [DurableClient] DurableTaskClient client,
            FunctionContext context)
    {   
        await ExecuteTriggerFunction.ExcuteChangeTrigger(changes, Table_TrackingDemo, client, _action, context, ["/post", Table_TrackingDemo]);
    }

    [Function(nameof(Table_TrackingDemo2_Function))]
    public async Task Table_TrackingDemo2_Function(
            [SqlTrigger(Table_TrackingDemo2, "SqlConnectionString", "lease_" + Table_TrackingDemo2)]
            IReadOnlyList<SqlChange<JsonObject>> changes,
            [DurableClient] DurableTaskClient client,
            FunctionContext context)
    {
        await ExecuteTriggerFunction.ExcuteChangeTrigger(changes, Table_TrackingDemo2, client, _action, context, ["/post", Table_TrackingDemo2]);
    }
}