using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp;

/// <summary>
/// Concrete implementation of <see cref="IDataSyncAction"/> that posts
/// filtered SQL change batches as JSON to a configured HTTP endpoint.
/// - Serializes the incoming <see cref="SqlChange{T}"/> items to JSON.
/// - Uses a 60-second cancellation timeout for the POST.
/// - Logs success and failure with concise details.
/// - Throws to signal the SQL trigger extension to retry on transient failures.
/// </summary>
public class HttpPostAction : IDataSyncAction
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<HttpPostAction> _logger;

    public HttpPostAction(HttpClient httpClient, ILogger<HttpPostAction> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    /// <summary>
    /// Executes the HTTP POST to the downstream route.
    /// Parameters:
    /// - parameters[0]: string route (e.g., "/post")
    /// - parameters[1]: table name for logging context
    /// </summary>
    public async Task ExecuteAction(IReadOnlyList<SqlChange<JsonObject>> changes, params object[] parameters)
    {
        // Serialize the list of change items to JSON for posting
        using StringContent content = new(JsonSerializer.Serialize(changes), System.Text.Encoding.UTF8, "application/json");

        // Ensure the POST does not hang indefinitely (960s timeout)
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(960));

        string route = parameters[0]?.ToString() ?? string.Empty;

        // Log the intent to post with basic context
        _logger.LogCritical("Posting {Count} change(s) for table {table} to route {Route}", changes?.Count ?? 0, parameters[1], route);

        // Perform the HTTP POST to the relative/absolute route; HttpClient base address should be configured elsewhere
        using HttpResponseMessage response = await _httpClient.PostAsync(route, content, cts.Token);

        // Fast path: success => log and return without throwing
        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning("Success: POST to {Route} for table {table} with response {Status}", route, parameters[1], (int)response.StatusCode);

            return;
        }

        #region Unhappy path

        // Capture a small error snippet from the response body for diagnostics
        string errorcontent = await response.Content.ReadAsStringAsync();

        int cutoff = 500;
        errorcontent = errorcontent.Length > cutoff ? errorcontent.Substring(0, cutoff) : errorcontent;

        _logger.LogError("POST for {table} to route {Route} failed with {Status}: {Snippet}", parameters[1], route, (int)response.StatusCode, errorcontent);

        // Retry on transient statuses:
        //  - 429 Too Many Requests
        //  - 408 Request Timeout
        //  - 5xx Server errors
        // Throwing here ensures:
        //  - Durable orchestration will kick in
        //  - The SQL trigger extension will not advance its checkpoint and will redeliver.
        //if ((int)response.StatusCode == 429 || (int)response.StatusCode == 408 || (int)response.StatusCode >= 500)
        //{
            throw new HttpRequestException($"HTTP {(int)response.StatusCode}: {errorcontent}");
        //}
        //else
        //{
        //    // Non-retryable statuses propagate with a "retry=false" marker.
        //    // This allows upstream logic to differentiate and schedule notifications only.
        //    throw new HttpRequestException($"retry=false - HTTP {response.StatusCode}: {errorcontent}");
        //}

        #endregion
    }
}
