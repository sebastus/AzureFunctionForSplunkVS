using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;

namespace AzureFunctionForSplunk
{
    public static class EhMetrics
    {
        [FunctionName("EhMetrics")]
        public static async Task Run([EventHubTrigger("%input-hub-name-metrics%", Connection = "hubConnection")]
            string[] messages, 
            TraceWriter log)
        {
            List<string> splunkEventMessages = MakeSplunkEventMessages(messages, log);

            string outputBinding = Utils.getEnvironmentVariable("outputBinding");
            if (outputBinding.ToUpper() == "HEC")
            {
                await Utils.obHEC(splunkEventMessages, log);
            }
            else
            {
                log.Info("No or incorrect output binding specified. No messages sent to Splunk.");
            }
        }

        private static List<string> MakeSplunkEventMessages(string[] messages, TraceWriter log)
        {
            Dictionary<string, string> MetricsCategories = new Dictionary<string, string>();

            var filename = Utils.getFilename("MetricsCategories.json");

            // log.Info($"File name of categories dictionary is: {filename}");

            try
            {
                MetricsCategories = Utils.GetDictionary(filename);
            }
            catch (Exception ex)
            {
                log.Error($"Error getting categories json file. {ex.Message}");
            }

            List<string> splunkEventMessages = new List<string>();

            foreach (var message in messages)
            {
                var converter = new ExpandoObjectConverter();
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(message, converter);

                var records = obj.records;
                foreach (var record in records)
                {
                    var resourceId = (string)record.resourceId;
                    var metricMessage = new MetricMessage(resourceId, record);

                    metricMessage.SplunkSourceType = Utils.GetDictionaryValue(metricMessage.ResourceType, MetricsCategories) ?? "amm:metrics";

                    string splunkEventMessage = metricMessage.GetSplunkEventFromMessage();

                    splunkEventMessages.Add(splunkEventMessage);
                }
            }

            return splunkEventMessages;
        }
    }
}
