// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.WebJobs.Script.Scale
{
    /// <summary>
    /// Layer over core scale provider interfaces, providing higher level services.
    /// </summary>
    public class FunctionsScaleManager
    {
        private readonly ITriggerScaleMonitorManager _monitorManager;
        private readonly ITriggerMetricsRepository _metricsRepository;
        private readonly ILogger _logger;

        public FunctionsScaleManager(ITriggerScaleMonitorManager monitorManager, ITriggerMetricsRepository metricsRepository, ILoggerFactory loggerFactory)
        {
            _monitorManager = monitorManager;
            _metricsRepository = metricsRepository;
            _logger = loggerFactory.CreateLogger(ScriptConstants.TraceSourceScale);
        }

        /// <summary>
        /// Get the current scale status (vote) by querying all active triggers for their
        /// scale status
        /// </summary>
        /// <param name="context">The context to use for the scale decision.</param>
        /// <returns>The scale vote.</returns>
        public async Task<ScaleVote> GetScaleStatusAsync(ScaleStatusContext context)
        {
            // get the collection of current metrics for each monitor
            var monitors = _monitorManager.GetMonitors();
            var monitorMetrics = await _metricsRepository.ReadAsync(monitors);

            _logger.LogInformation($"Computing scale status (WorkerCount={context.WorkerCount})");
            _logger.LogInformation($"{monitorMetrics.Count} triggers to sample");

            // for each monitor, ask it to return its scale status (vote) based on
            // the metrics and context info (e.g. worker count)
            List<ScaleVote> votes = new List<ScaleVote>();
            foreach (var pair in monitorMetrics)
            {
                var monitor = pair.Key;
                var metrics = pair.Value;

                context.Metrics = metrics;
                var result = monitor.GetScaleStatus(context);

                var vote = result.Vote;
                _logger.LogInformation($"Function '{monitor.FunctionId}' voted '{vote.ToString()}'");
                votes.Add(vote);
            }

            if (votes.Any())
            {
                // aggregate all the votes into a single vote
                if (votes.Any(p => p == ScaleVote.ScaleOut))
                {
                    // scale out if at least 1 trigger requires it
                    _logger.LogInformation("Scaling out based on votes");
                    return ScaleVote.ScaleOut;
                }
                else if (context.WorkerCount > 0 && votes.All(p => p == ScaleVote.ScaleIn))
                {
                    // scale in only if all triggers vote scale in
                    _logger.LogInformation("Scaling in based on votes");
                    return ScaleVote.ScaleIn;
                }
            }
            else if (context.WorkerCount > 0)
            {
                // if no functions exist or are enabled we'll
                // scale in
                _logger.LogInformation("No Functions or Scale Votes so scaling in");
                return ScaleVote.ScaleIn;
            }

            return ScaleVote.None;
        }
    }
}
