using System.Net.Http.Headers;
using SqlDataIntegrationFunctionTriggerApp;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using SqlDataIntegrationFunctionTriggerApp.Models;

FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddApplicationInsightsTelemetryWorkerService();
builder.Services.ConfigureFunctionsApplicationInsights();

builder.Services.AddHttpClient("Default", client =>
{
    string? baseUrl = builder.Configuration["HttpPostBaseUrl"];
    client.BaseAddress = new Uri(baseUrl ?? throw new InvalidOperationException("HttpPostBaseUrl configuration is missing."));
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

builder.Services.AddOptions<AppSettings>()
    .Configure<IConfiguration>((opts, cfg) =>
    {
        opts.DurableFunctionRetryIntervalMinutes = cfg.GetValue<int>("DurableFunctionRetryIntervalMinutes");
        opts.TotalRetryTimeOutHours = cfg.GetValue<int>("TotalRetryTimeOutHours");
        opts.NotifyOnRetryCount = cfg.GetValue<int>("NotifyOnRetryCount");
        opts.SqlConnectionString = cfg.GetConnectionString("SqlConnectionString") ?? cfg["SqlConnectionString"];
    });

builder.Build().Run();
