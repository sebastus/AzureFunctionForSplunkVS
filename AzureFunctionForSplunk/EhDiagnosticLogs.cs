using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionForSplunk
{
    public static class EhDiagnosticLogs
    {
        [FunctionName("EhDiagnosticLogs")]
        public static async Task Run(
            [EventHubTrigger("%input-hub-name-diagnostics-logs%", Connection = "hubConnection")]string[] messages,
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
            Dictionary<string, string> DiagnosticLogCategories = new Dictionary<string, string>();

            var filename = Utils.getFilename("DiagnosticLogCategories.json");

            // log.Info($"File name of categories dictionary is: {filename}");

            try
            {
                DiagnosticLogCategories = Utils.GetDictionary(filename);
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
                    var resourceId = record.resourceId;

                    var splunkEventMessage = new DiagnosticLogMessage(resourceId, record);

                    var resourceType = splunkEventMessage.ResourceType;

                    string category;
                    try
                    {
                        category = record.category;
                    } catch
                    {
                        category = "none";
                    }
                    var sourceType = Utils.GetDictionaryValue(resourceType.ToUpper() + "/" + category.ToUpper(), DiagnosticLogCategories) ?? "amdl:diagnostic";

                    splunkEventMessage.SetSourceType(sourceType);

                    splunkEventMessages.Add(splunkEventMessage.GetSplunkEventFromMessage());
                }
            }

            return splunkEventMessages;
        }
    }
}
