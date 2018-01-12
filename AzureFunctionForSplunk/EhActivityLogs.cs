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
    public static class EhActivityLogs
    {
        [FunctionName("EhActivityLogs")]
        public static async Task Run(
            [EventHubTrigger("%input-hub-name-activity-logs%", Connection = "hubConnection")]string[] messages, 
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
            Dictionary<string, string> ActivityLogCategories = new Dictionary<string, string>();

            var filename = Utils.getFilename("ActivityLogCategories.json");

            // log.Info($"File name of categories dictionary is: {filename}");

            try
            {
                ActivityLogCategories = Utils.GetDictionary(filename);
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

                string operationName;
                foreach (var record in records)
                {
                    operationName = record.operationName;

                    var splits = operationName.Split('/');

                    string splunkEventMessage = "";
                    string sourceType = "amal:activitylog";

                    if (splits.Length < 3)
                    {
                        // ASC Recommendation
                        sourceType = Utils.GetDictionaryValue("ascrecommendation", ActivityLogCategories) ?? "amal:asc:recommendation";
                    }
                    else if (splits.Length >= 3)
                    {
                        var provider = splits[0].ToUpper();
                        var type = splits[1].ToUpper();
                        var operation = splits[2].ToUpper();

                        switch (provider)
                        {
                            case "MICROSOFT.SERVICEHEALTH":
                                sourceType = Utils.GetDictionaryValue("servicehealth", ActivityLogCategories) ?? "amal:servicehealth";
                                break;

                            case "MICROSOFT.RESOURCEHEALTH":
                                sourceType = Utils.GetDictionaryValue("resourcehealth", ActivityLogCategories) ?? "amal:resourcehealth";
                                break;

                            case "MICROSOFT.INSIGHTS":
                                if (type == "AUTOSCALESETTINGS")
                                {
                                    sourceType = Utils.GetDictionaryValue("autoscalesettings", ActivityLogCategories) ?? "amal:autoscalesettings";
                                }
                                else if (type == "ALERTRULES")
                                {
                                    sourceType = Utils.GetDictionaryValue("ascalert", ActivityLogCategories) ?? "amal:ascalert";
                                }
                                else
                                {
                                    sourceType = Utils.GetDictionaryValue("insights", ActivityLogCategories) ?? "amal:insights";
                                }
                                break;
                            case "MICROSOFT.SECURITY":
                                if (type == "APPLICATIONWHITELISTINGS")
                                {
                                    if (operation == "ACTION")
                                    {
                                        sourceType = Utils.GetDictionaryValue("ascalert", ActivityLogCategories) ?? "amal:asc:alert";
                                    }
                                    else
                                    {
                                        sourceType = Utils.GetDictionaryValue("security", ActivityLogCategories) ?? "amal:security";
                                    }
                                }
                                else if (type == "LOCATIONS")
                                {
                                    sourceType = Utils.GetDictionaryValue("security", ActivityLogCategories) ?? "amal:security";
                                }
                                else if (type == "TASKS")
                                {
                                    sourceType = Utils.GetDictionaryValue("ascrecommendation", ActivityLogCategories) ?? "amal:asc:recommendation";
                                }
                                break;
                            default:
                                {
                                    // administrative category
                                    sourceType = Utils.GetDictionaryValue("administrative", ActivityLogCategories) ?? "amal:administrative"; 
                                    break;
                                }
                        }
                    }

                    splunkEventMessage = ProcessStandardMessage(record, sourceType, log);

                    splunkEventMessages.Add(splunkEventMessage);
                }
            }
            return splunkEventMessages;
        }

        public static string ProcessStandardMessage(dynamic record, string sourceType, TraceWriter log)
        {
            var resourceId = (string)record.resourceId;
            var operationName = (string)record.operationName;

            var message = new ActivityLogMessage(resourceId, sourceType, record);

            return message.GetSplunkEventFromMessage();

        }
    }
}
