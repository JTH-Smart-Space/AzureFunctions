using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using System;
using System.Text;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Core.Pipeline;
using Azure;

namespace JTHSmartSpace.AzureFunctions
{
    public static class PropagateStateChangeToTsi
    { 

        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        private static DigitalTwinsClient dtClient;

        [FunctionName("PropagateStateChangeToTsi")]
        public static async Task Run(
            [EventHubTrigger("twins-event-hub", Connection = "EventHubAppSetting-Twins")]EventData myEventHubMessage,
            [EventHub("tsi-event-hub", Connection = "EventHubAppSetting-TSI")]IAsyncCollector<string> outputEvents,
            ILogger log)
        {
            if (adtServiceUrl == null) {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            DefaultAzureCredential credentials = new DefaultAzureCredential();
            dtClient = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions{
                Transport = new HttpClientTransport(httpClient)
            });

            string twinId = myEventHubMessage.Properties["cloudEvents:subject"].ToString();
            JObject message = (JObject)JsonConvert.DeserializeObject(Encoding.UTF8.GetString(myEventHubMessage.Body));
            log.LogInformation($"Reading event: '{message}' from twin '{twinId}'.");

            bool twinIsState = await TwinIsState(twinId);

            // TODO: Only execute the below (propagating data into TSI) if the updated twin is an instance of the State interface

            // Read values that are replaced or added
            var tsiUpdate = new Dictionary<string, object>();
            foreach (var operation in message["patch"])
            {
                if (operation["op"].ToString() == "replace" || operation["op"].ToString() == "add")
                {
                    //Convert from JSON patch path to a flattened property for TSI
                    //Example input: /Front/Temperature
                    //        output: Front.Temperature
                    string path = operation["path"].ToString().Substring(1);
                    path = path.Replace("/", ".");
                    tsiUpdate.Add(path, operation["value"]);
                }
            }
            // Send an update if updates exist
            if (tsiUpdate.Count > 0)
            {
                tsiUpdate.Add("$dtId", myEventHubMessage.Properties["cloudEvents:subject"]);
                await outputEvents.AddAsync(JsonConvert.SerializeObject(tsiUpdate));
            }
        }

        private static async Task<bool> TwinIsState(string twinId) {
            string stateQuery = $"SELECT State FROM DIGITALTWINS State WHERE IS_OF_MODEL('dtmi:digitaltwins:rec_3_3:core:State;1') AND State.$dtId = '{twinId}'";
            AsyncPageable<string> results = dtClient.QueryAsync(stateQuery);
            bool matchingQueryResult = false;
            try
            {
                await foreach(string twin in results)
                {
                    matchingQueryResult = true;
                    break;
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error {ex.Status}, {ex.ErrorCode}, {ex.Message}");
                throw;
            }
            return matchingQueryResult;
        }
    }
}