using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;

namespace SqlDataIntegrationFunctionTriggerApp;

public class CleanupFunction
{
    [Function("CleanupFunction")]
    public async Task Run([TimerTrigger("0 0 4 * * Sun")] TimerInfo myTimer, [DurableClient] DurableTaskClient client)//"0 */5 * * * *" "0 0 4 * * Sun"
    {
        // Purge orchestration instances created before 7 days ago
        int completedDays = Convert.ToInt32(Environment.GetEnvironmentVariable("KeepInstanceCompletedHistoryDays"));

        DateTimeOffset cutoff = DateTimeOffset.UtcNow.AddDays(-completedDays);

        // Use the correct enum type for status filtering
        PurgeResult result = await client.PurgeInstancesAsync(
            DateTimeOffset.MinValue,
            cutoff,
            statuses: new List<OrchestrationRuntimeStatus> { OrchestrationRuntimeStatus.Completed }
        );

        int failedDays = Convert.ToInt32(Environment.GetEnvironmentVariable("KeepInstanceFailedHistoryDays"));

        DateTimeOffset cutoff2 = DateTimeOffset.UtcNow.AddDays(-completedDays);

        // Use the correct enum type for status filtering
        result = await client.PurgeInstancesAsync(
            DateTimeOffset.MinValue,
            cutoff2,
            statuses: new List<OrchestrationRuntimeStatus> { OrchestrationRuntimeStatus.Terminated, OrchestrationRuntimeStatus.Suspended,
            OrchestrationRuntimeStatus.Failed }
        );

        await client.Entities.CleanEntityStorageAsync(new CleanEntityStorageRequest() 
        {
            ReleaseOrphanedLocks = true,
            RemoveEmptyEntities = true 
        });
    }
}