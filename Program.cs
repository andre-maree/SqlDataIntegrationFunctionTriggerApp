using System.Net.Http.Headers;
using SqlDataIntegrationFunctionTriggerApp;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddApplicationInsightsTelemetryWorkerService();
builder.Services.ConfigureFunctionsApplicationInsights();

builder.Services.AddHttpClient("Default", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["HttpPostBaseUrl"]);
    client.DefaultRequestHeaders.Accept.Clear();
    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
});
builder.Services.AddTransient<HttpPostAction>(sp =>
{
    IHttpClientFactory httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
    HttpClient client = httpClientFactory.CreateClient("Default");
    var logger = sp.GetRequiredService<ILogger<HttpPostAction>>();
    return new HttpPostAction(client, logger);
});
builder.Services.AddTransient<IDataSyncAction>(sp => sp.GetRequiredService<HttpPostAction>());

builder.Build().Run();
