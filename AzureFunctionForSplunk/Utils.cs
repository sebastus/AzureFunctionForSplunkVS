using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunctionForSplunk
{
    public class Utils
    {
        public static string getEnvironmentVariable(string name)
        {
            var result = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
            if (result == null)
                return "";

            return result;
        }

        public static Dictionary<string, string> GetDictionary(string filename)
        {
            Dictionary<string, string> dictionary;
            try
            {
                string json = File.ReadAllText(filename);

                dictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
            }
            catch (Exception ex)
            {
                dictionary = new Dictionary<string, string>();
            }

            return dictionary;
        }

        public static string GetDictionaryValue(string key, Dictionary<string, string> dictionary)
        {
            string value = "";
            if (dictionary.TryGetValue(key, out value))
            {
                return value;
            } else
            {
                return null;
            }
        }

        public class SingleHttpClientInstance
        {
            private static readonly HttpClient HttpClient;

            static SingleHttpClientInstance()
            {
                HttpClient = new HttpClient();
            }

            public static async Task<HttpResponseMessage> SendToSplunk(HttpRequestMessage req)
            {
                HttpResponseMessage response = await HttpClient.SendAsync(req);
                return response;
            }
        }

        public static async Task obHEC(List<string> standardizedEvents, TraceWriter log)
        {
            string splunkAddress = Utils.getEnvironmentVariable("splunkAddress");
            string splunkToken = Utils.getEnvironmentVariable("splunkToken");
            if (splunkAddress.Length == 0 || splunkToken.Length == 0)
            {
                log.Error("Values for splunkAddress and splunkToken are required.");
                return;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback =
            new System.Net.Security.RemoteCertificateValidationCallback(
                delegate { return true; });

            var newClientContent = new StringBuilder();
            foreach (string item in standardizedEvents)
            {
                newClientContent.Append(item);
            }
            var client = new SingleHttpClientInstance();
            try
            {
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, splunkAddress);
                req.Headers.Accept.Clear();
                req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                req.Headers.Add("Authorization", "Splunk " + splunkToken);
                req.Content = new StringContent(newClientContent.ToString(), Encoding.UTF8, "application/json");
                HttpResponseMessage response = await SingleHttpClientInstance.SendToSplunk(req);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    log.Error($"StatusCode from Splunk: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                }
            }
            catch (System.Net.Http.HttpRequestException e)
            {
                log.Error($"Error: \"{e.InnerException.Message}\" was caught while sending to Splunk. Is the Splunk service running?");
            }
            catch (Exception f)
            {
                log.Error($"Error \"{f.InnerException.Message}\" was caught while sending to Splunk. Unplanned exception.");
            }
        }

    }
}
