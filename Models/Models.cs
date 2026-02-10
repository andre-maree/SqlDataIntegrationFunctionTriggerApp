namespace SqlDataIntegrationFunctionTriggerApp.Models
{
    public sealed class RetryOrchestrationObject
    {
        public int IntervalMinutes { get; set; }
        public SqlActivityObject SqlActivityObject { get; set; }
    }

    public sealed class SqlActivityObject
    {
        public int RetryCount { get; set; }
        public DateTime StartDate { get; set; } = DateTime.UtcNow;
        public TimeSpan RetryTimeoutSpan { get; set; }
    }
}
