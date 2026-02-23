using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp
{
    public static class LoggerHelper
    {
        public static void LogGrey(this ILogger logger, string message, params object?[] args)
        {
            logger.LogCritical(message, args);
        }

        public static void LogRed(this ILogger logger, string message, params object?[] args)
        {
            logger.LogError(message, args);
        }

        public static void LogOrange(this ILogger logger, string message, params object?[] args)
        {
            logger.LogWarning(message, args);
        }
    }
}
