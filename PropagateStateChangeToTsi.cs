using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using System;
using System.Text;
using System.Text.Json;
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

            // Retrieve twin that caused message to be sent; we need it's name for the TSI message
            Response<BasicDigitalTwin> twinResponse = await dtClient.GetDigitalTwinAsync<BasicDigitalTwin>(twinId);
            BasicDigitalTwin twin = twinResponse.Value;

            // Extract name of capability twin 
            string tsiProperty = "";
            if (twin.Contents.ContainsKey("name") && twin.Contents["name"] is JsonElement) {
                tsiProperty = ((JsonElement)twin.Contents["name"]).ToString().Trim();
            }
            // Retrieve twin that this capability applies to; construct TSI payload with this as path
            string capabilityParentId = await FindCapabilityParentAsync(twinId, log);

            // If we have both a capability parent id and a TSI property, proceed to create TSI event
            if (tsiProperty != "" && capabilityParentId != null) {
                var tsiUpdate = new Dictionary<string, object>();
                foreach (var operation in message["patch"])
                {
                    if (operation["op"].ToString() == "replace" || operation["op"].ToString() == "add")
                    {
                        tsiUpdate.Add(tsiProperty, operation["value"]);
                    }
                }
                // Send an update if updates exist
                if (tsiUpdate.Count > 0)
                {
                    tsiUpdate.Add("$dtId", capabilityParentId);
                    log.LogInformation($"SENDING: {JsonConvert.SerializeObject(tsiUpdate)}");
                    await outputEvents.AddAsync(JsonConvert.SerializeObject(tsiUpdate));
                }
            }
        }

        public static async Task<string> FindCapabilityParentAsync(string capability, ILogger log)
        {
            // Find parent to which the capability applies, using isCapabilityOf relationships
            try
            {
                AsyncPageable<BasicRelationship> rels = dtClient.GetRelationshipsAsync<BasicRelationship>(capability, "isCapabilityOf");
                await foreach (BasicRelationship relationship in rels)
                {
                    return relationship.TargetId;
                }
            }
            catch (RequestFailedException exc)
            {
                log.LogInformation($"*** Error in retrieving capability parent:{exc.Status}:{exc.Message}");
            }
            return null;
        }
    }
}