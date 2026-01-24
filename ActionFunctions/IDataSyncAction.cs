using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;

namespace DataChangeTrackingFunctionApp
{
    public interface IDataSyncAction
    {
        Task ExecuteAction(IReadOnlyList<SqlChange<JsonObject>> changes, params object[] parameters);
    }
}