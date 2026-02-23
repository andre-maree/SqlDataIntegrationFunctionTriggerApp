using Microsoft.Extensions.Logging;

namespace SqlDataIntegrationFunctionTriggerApp
{
    /// <summary>
    /// Convenience <see cref="ILogger"/> extensions with names mapped to specific log levels.
    /// Note: these helpers do not control console colors; providers decide any coloring.
    /// </summary>
    public static class LoggerHelper
    {
        /// <summary>Logs using <see cref="LogLevel.Critical"/>.</summary>
        public static void LogGrey(this ILogger logger, string message, params object?[] args)
        {
            logger.LogCritical(message, args);
        }

        /// <summary>Logs using <see cref="LogLevel.Error"/>.</summary>
        public static void LogRed(this ILogger logger, string message, params object?[] args)
        {
            logger.LogError(message, args);
        }

        /// <summary>Logs using <see cref="LogLevel.Warning"/>.</summary>
        public static void LogOrange(this ILogger logger, string message, params object?[] args)
        {
            logger.LogWarning(message, args);
        }
    }
}
