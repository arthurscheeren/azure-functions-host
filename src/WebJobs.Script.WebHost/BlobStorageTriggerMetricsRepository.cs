// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Azure.WebJobs.Script.Scale;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Script.WebHost
{
    public class BlobStorageTriggerMetricsRepository : ITriggerMetricsRepository
    {
        // TODO: Decide on the right number of samples to maintain per trigger
        private const int MaxMetricsCount = 5;
        private readonly IConfiguration _configuration;
        private readonly IHostIdProvider _hostIdProvider;
        private CloudBlockBlob _metricsBlob;

        public BlobStorageTriggerMetricsRepository(IConfiguration configuration, IHostIdProvider hostIdProvider)
        {
            _configuration = configuration;
            _hostIdProvider = hostIdProvider;
        }

        private CloudBlockBlob MetricsBlob
        {
            get
            {
                if (_metricsBlob == null)
                {
                    string storageConnectionString = _configuration.GetWebJobsConnectionString(ConnectionStringNames.Storage);
                    CloudStorageAccount account = null;
                    if (!string.IsNullOrEmpty(storageConnectionString) &&
                        CloudStorageAccount.TryParse(storageConnectionString, out account))
                    {
                        string hostId = _hostIdProvider.GetHostIdAsync(CancellationToken.None).GetAwaiter().GetResult();
                        CloudBlobClient blobClient = account.CreateCloudBlobClient();
                        var blobContainer = blobClient.GetContainerReference(ScriptConstants.AzureWebJobsHostsContainerName);
                        string blobPath = $"scale/{hostId}/metrics.json";
                        _metricsBlob = blobContainer.GetBlockBlobReference(blobPath);
                    }
                }

                // TODO: handle errors if we can't connect to storage, etc.
                return _metricsBlob;
            }
        }

        public async Task<IDictionary<ITriggerScaleMonitor, IList<TriggerMetrics>>> ReadAsync(IEnumerable<ITriggerScaleMonitor> monitors)
        {
            // load the existing metrics from blob
            string content = null;
            if (await MetricsBlob.ExistsAsync())
            {
                content = await MetricsBlob.DownloadTextAsync();
            }
            JObject rawMetrics = JObject.Parse(content);

            Dictionary<ITriggerScaleMonitor, IList<TriggerMetrics>> result = new Dictionary<ITriggerScaleMonitor, IList<TriggerMetrics>>();
            foreach (var monitor in monitors)
            {
                List<TriggerMetrics> metrics = new List<TriggerMetrics>();
                string key = GetKey(monitor);
                JArray rawMontitorMetrics = (JArray)rawMetrics[key];

                // TODO: handle case where this might not be generic
                var monitorInterfaceType = monitor.GetType().GetInterfaces().SingleOrDefault(p => p.IsGenericType && p.GetGenericTypeDefinition() == typeof(ITriggerScaleMonitor<>));
                Type metricsType = monitorInterfaceType.GetGenericArguments()[0];

                foreach (JObject currMetrics in rawMontitorMetrics)
                {
                    var deserializedMetrics = (TriggerMetrics)currMetrics.ToObject(metricsType);
                    metrics.Add(deserializedMetrics);
                }

                result[monitor] = metrics;
            }

            return result;
        }

        public async Task WriteAsync(IDictionary<ITriggerScaleMonitor, TriggerMetrics> metricsMap)
        {
            // load the existing metrics if present
            JObject rawMetrics = null;
            string content = null;
            if (await MetricsBlob.ExistsAsync())
            {
                content = await MetricsBlob.DownloadTextAsync();
                rawMetrics = JObject.Parse(content);
            }
            else
            {
                rawMetrics = new JObject();
            }

            // When writing the metrics blob back after adding the current metrics, we'll be filtering out any metrics for functions that
            // no longer exist, are disabled, etc. So obsolete metrics will be purged naturally (e.g. when functions are deleted,
            // disabled, etc.)
            JObject newMetrics = new JObject();
            foreach (var pair in metricsMap)
            {
                // get the current metrics for this provider if present
                string key = GetKey(pair.Key);
                JArray currMetrics = (JArray)rawMetrics[key];
                if (currMetrics == null)
                {
                    currMetrics = new JArray();
                }

                if (currMetrics.Count == MaxMetricsCount)
                {
                    // if we're at the max size remove the oldest before appending
                    currMetrics.RemoveAt(0);
                }

                // TODO: Should we also apply a "max age" filter to the timestamp, to purge very old
                // metrics? We probably don't want to be keeping metrics around for too long. Otherwise
                // we might come online after being down for a while, and make scale decisions on very
                // old data that no longer reflects reality.

                // append the metrics for the current monitor
                currMetrics.Add(JObject.FromObject(pair.Value));

                // add the updated metrics
                newMetrics[key] = currMetrics;
            }

            // persist the metrics
            string json = newMetrics.ToString();
            await MetricsBlob.UploadTextAsync(json);
        }

        private string GetKey(ITriggerScaleMonitor monitor)
        {
            // Form a unique key which will allow us to rehydrate and pair
            // with the correct monitor instance
            return $"{monitor.FunctionId}-{monitor.TriggerType}-{monitor.ResourceId}".ToLower();
        }
    }
}
