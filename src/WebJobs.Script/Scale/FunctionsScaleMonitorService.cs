// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Script.Scale
{
    /// <summary>
    /// Service responsible for taking periodic trigger metrics samples and persisting them.
    /// </summary>
    public class FunctionsScaleMonitorService : IHostedService, IDisposable
    {
        private readonly IPrimaryHostStateProvider _primaryHostStateProvider;
        private readonly ITriggerScaleMonitorManager _monitorManager;
        private readonly ITriggerMetricsRepository _metricsRepository;
        private readonly IEnvironment _environment;
        private readonly ILogger _logger;
        private readonly Timer _timer;
        private readonly TimeSpan _interval;

        public FunctionsScaleMonitorService(ITriggerScaleMonitorManager monitorManager, ITriggerMetricsRepository metricsRepository, IPrimaryHostStateProvider primaryHostStateProvider, IEnvironment environment, ILoggerFactory loggerFactory)
        {
            _monitorManager = monitorManager;
            _metricsRepository = metricsRepository;
            _primaryHostStateProvider = primaryHostStateProvider;
            _environment = environment;
            _logger = _logger = loggerFactory.CreateLogger(ScriptConstants.TraceSourceScale);

            // TODO: make interval configurable via options
            _interval = TimeSpan.FromSeconds(10);
            _timer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (_environment.IsRuntimeScaleMonitoringEnabled())
            {
                // start the timer by setting the due time
                _timer.Change((int)_interval.TotalMilliseconds, Timeout.Infinite);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            // stop the timer if it has been started
            _timer.Change(Timeout.Infinite, Timeout.Infinite);

            return Task.CompletedTask;
        }

        private async void OnTimer(object state)
        {
            try
            {
                // TODO: we're doing a check here to see if runtime scale monitoring is enabled.
                // However, we shouldn't even be running this service/timer if it isn't enabled.
                if (_primaryHostStateProvider.IsPrimary)
                {
                    var monitors = _monitorManager.GetMonitors();

                    // take a metrics sample for each monitor
                    var metricsMap = new Dictionary<ITriggerScaleMonitor, TriggerMetrics>();
                    foreach (var monitor in monitors)
                    {
                        var metrics = await monitor.GetMetricsAsync();
                        metricsMap[monitor] = metrics;

                        // log the metrics json to provide visibility into trigger activity
                        var json = JsonConvert.SerializeObject(metrics);
                        _logger.LogInformation($"Metrics sample for {monitor.FunctionId}: {json}");
                    }

                    // persist the metrics samples
                    await _metricsRepository.WriteAsync(metricsMap);
                }

                var timer = _timer;
                if (timer != null)
                {
                    try
                    {
                        _timer.Change((int)_interval.TotalMilliseconds, Timeout.Infinite);
                    }
                    catch (ObjectDisposedException)
                    {
                        // might race with dispose
                    }
                }
            }
            catch (Exception exc) when (!exc.IsFatal())
            {
                _logger.LogError(exc, "Failed to collect/persist metrics sample.");
            }
        }

        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}
