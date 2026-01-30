# SqlDataIntegrationFunctionTriggerApp

Azure Functions app (.NET 8, isolated worker) that receives SQL Server data changes with the SQL Server Change Tracking/CDC feature enabled. The changed data payload is received by an Azure Function via the SqlTrigger function input binding.

Only the latest data changes are delivered. This is not a change log; rather, it provides the current state of changed rows in near real time. The app processes the changes, removes unneeded columns , HTTP posts changes to an endpoint, and uses Durable Functions for retries and notifications. Durable entity state is used for persisting the last error info.

## Features
- Receive SQL Server data changes per table via a C# Azure Function SqlTrigger.
- Durable Functions:
  - Orchestration for retry: `RetryOrchestration`
  - Orchestration for error notifications:`NotifyOrchestrator`
  - Durable Entities for state: `LastError`, `AllowedColumns`.
- Configurable allowed columns (per table) that can be set in the app config, and an additional allowed columns that can be set via an Api call.
- Cleanup job to purge old orchestration history and entity state.
- Event-driven, no polling required: This app uses the Azure SQL trigger in an event-driven manner. When SQL Change Tracking/CDC commits changes, the `SqlTrigger` delivers them to the function automatically. You do not need to write custom polling loops; the extension handles lease management, batching, and delivery.

## How the SQL Change Tracking/CDC feature works with an Azure Functions SQL trigger
- The Azure Functions SQL trigger extension manages delivery.
- It maintains a high-water mark and leases per table to ensure ordered, exactly-once batch delivery.
- It polls on the configured interval (Sql_Trigger_PollingIntervalMs) and pulls changes up to Sql_Trigger_MaxBatchSize, respecting Sql_Trigger_MaxChangesPerWorker.
- When new changes are available, it invokes your SqlTrigger function with a batch of current-row states.
- If the function completes successfully, the checkpoint advances and the next batch is fetched.
- If the function fails, the extension won’t advance the checkpoint; on the next poll, it will attempt delivery of the same batch again, always having the latest committed data.
- The fuction SQL trigger will be called 5 times if it keeps failing to post the data. Failures are saved via the `LastError` entity.
- The `RetryOrchestration` is started on the occurance of the first failed attempt. It will monitor and take over when the built-in retry of 5 times is exhausted. The orchestration will retry posting the data based on the configured retry interval and maximum number of retries.
- `NotifyOnRetryCount` can be set in the `local.settings.json` file. When the orchestration reaches this retry count number, it will start the `NotifyOrchestrator` to send a notification (e.g., email) about the persistent failure. Note: The `NotifyOnRetryCount` setting is only used in the `RetryOrchestration`and is not checked by the built-in 5 retries of the SQL function trigger.

## Serverless app that can be used for data integration
This app runs serverlessly on Azure Functions, scaling automatically based on load with minimal operational overhead. It is well-suited for integration scenarios where database changes must be propagated to downstream systems in near real time. Common patterns include:
- Posting change events to HTTP APIs or webhooks
- Enqueuing messages to queues/topics
  
## Prerequisites
- .NET 8 SDK
- Azure Functions Core Tools (for local run)
- SQL Server with Change Tracking enabled and the demo tables created (see `sql.txt`).

## Configuration
This is the config file `local.settings.json` (excluded from deployment). The setting `AllowedColumns_dbo.TrackingDemo` is not required, if it is included, the columns will be projected. If it is omitted then all columns will be allowed:

```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
        "WEBSITE_SITE_NAME": "SqlDataIntegrationFunctionTriggerApp",
        "SqlConnectionString": "<your-connection-string>",
        "Sql_Trigger_MaxBatchSize": 1000,
        "Sql_Trigger_PollingIntervalMs": 5000,
        "Sql_Trigger_MaxChangesPerWorker": 10000,
        "HttpPostBaseUrl": "https://httpbin.org/",
        "DurableFunctionRetryIntervalMinutes": 15,
        "AllowedColumns_dbo.TrackingDemo": "Id,Name,LastUpdate",
        "NotifyOnRetryCount": 10,
        "MaxNumberOfRetries" : 100,
        "KeepInstanceCompletedHistoryDays": 7,
        "KeepInstanceFailedHistoryDays": 30
  }
}
```

Notes:
- The `AllowedColumns_dbo.TrackingDemo` setting is used to project columns per table, in this case, for the table `dbo.TrackingDemo`
- Column projection can also be set by an API call, use `AllowedColumns` entity via the `SaveClientAllowedColumns` function.

## Getting Started
1. Create demo tables in SQL: see `SqlScripts/sql.txt`.
2. Enable SQL Change Tracking or CDC for the database/tables: see `SqlScripts/sql.txt`.
3. Restore/build:
   - `dotnet restore`
   - `dotnet build`
4. Run locally:
   - Press start in the IDE.
5. Trigger changes: insert/update/delete rows in tracked tables: see `SqlScripts/sql.txt`.
6. Observe functions:
   - SQL trigger functions (under `SqlTriggerFunctions`) deliver change batches and delegate processing to `ExecuteTriggerFunction`.
   - `ActionFunctions/HttpPostAction` posts JSON to the configured HTTP endpoint.
   - Failures are saved via the `LastError` entity.
   - `NotifyOrchestrator` runs when `RetryCount` hits `NotifyOnRetryCount` or when a non-retryable HTTP statuscode is returned.
   - `CleanupFunction` purges old history and entity state per schedule.

## Project structure (key parts)
- `SqlTriggerFunctions/ExecuteTriggerFunction.cs`: filters allowed columns and invokes actions for change batches.
- `ActionFunctions/HttpPostAction.cs`: posts payloads to downstream HTTP endpoints.
- `RetryFunctions/RetryFunctions.cs`: durable orchestration (`RetryOrchestration`) and activity for retry logic.
- `EntityFunctions/EntityFunctions.cs`: durable entities (`LastError`, `AllowedColumns`, `RetryCount`).
- `CleanupFunction/CleanupFunction.cs`: scheduled cleanup (e.g., Sundays 4 AM) purging instance history and entity storage.

## HTTP Endpoints
- `SaveClientAllowedColumns`: set allowed columns per table.
- `GetClientAllowedColumns`: read allowed columns for a table.
- `/post`: sample endpoint to receive posted change payloads (configure `HttpPostBaseUrl`).

## Tuning
- SQL trigger polling/lease via `host.json` (extension settings).
- Orchestration intervals via `DurableFunctionRetryIntervalMinutes`.
- Retry policies in `NotifyOrchestrator`.

## Deployment
Deploy to Azure Functions (Consumption or Premium). Ensure app settings include:
- `WEBSITE_SITE_NAME` (set by Azure automatically on deploy).
- All values from `local.settings.json` moved into Azure Configuration.

## Final deployment checklist and enabling target based scaling (TBS)
- Enabling TBS is only needed for more aggressive scaling if a large backlog of change rows in a lease table is a likely problem in terms of speedy processing.
- SQL Side: Verify Change Tracking is ON with a retention period longer than your longest intended retry (e.g., 7 days).
- Ensure your DurableOrchestration uses RetryOptions with maxNumberOfAttempts: -1.
- Cloud Side: Deploy to the Flex Consumption plan and toggle Runtime Scale Monitoring to ON in the Azure Portal.
- Security: Use Managed Identity to connect to SQL so that there will be no failures due to an expired password.

## License
MIT
