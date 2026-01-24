using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Extensions.Logging;

namespace DataChangeTrackingFunctionApp
{
    public class HttpPostAction : IDataSyncAction
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<HttpPostAction> _logger;

        public HttpPostAction(HttpClient httpClient, ILogger<HttpPostAction> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task ExecuteAction(IReadOnlyList<SqlChange<JsonObject>> changes, params object[] parameters)
        {
            using StringContent content = new(JsonSerializer.Serialize(changes), System.Text.Encoding.UTF8, "application/json");

            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(60));

            string route = parameters[0]?.ToString() ?? string.Empty;

            _logger.LogCritical("Posting {Count} change(s) for table {table} to route {Route}", changes?.Count ?? 0, parameters[1], route);

            using HttpResponseMessage response = await _httpClient.PostAsync(route, content, cts.Token);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Success: POST to {Route} for table {table} with response {Status}", route, parameters[1], (int)response.StatusCode);

                return;
            }

            string errorcontent = await response.Content.ReadAsStringAsync();

            errorcontent = errorcontent.Length > 250 ? errorcontent.Substring(0, 250) : errorcontent;

            _logger.LogError("POST for {table} to route {Route} failed with {Status}: {Snippet}", parameters[1], route, (int)response.StatusCode, errorcontent);

            // Retry on transient statuses
            if ((int)response.StatusCode == 429 || (int)response.StatusCode == 408 || (int)response.StatusCode >= 500)
            {
                throw new HttpRequestException($"HTTP {(int)response.StatusCode}: {errorcontent}");
            }
            else
            {
                throw new HttpRequestException($"retry=false - HTTP {response.StatusCode}: {errorcontent}");
            }
        }
    }
}
