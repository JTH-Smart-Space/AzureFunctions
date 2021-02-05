// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using System.Net.Http;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Core.Pipeline;
using Newtonsoft.Json.Linq;
using Azure.DigitalTwins.Core.Serialization;
using Newtonsoft.Json;
using RealEstateCore;

namespace JTHSmartSpace.AzureFunctions
{
    public static class IngestRecMessages
    {

        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        [FunctionName("IngestRecMessages")]
        public static async void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            if (adtServiceUrl == null) {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            DefaultAzureCredential credentials = new DefaultAzureCredential();
            DigitalTwinsClient dtClient = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions{
                Transport = new HttpClientTransport(httpClient)
            });

            JObject payload = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
            if (payload.ContainsKey("body")) {
                JObject payloadBody = (JObject)payload["body"];

                if (payloadBody.ContainsKey("format") && ((string)payloadBody["format"]).StartsWith("rec3.2")) {
                    RecEdgeMessage recMessage = JsonConvert.DeserializeObject<RecEdgeMessage>(payloadBody.ToString());
                    foreach (Observation observation in recMessage.observations)
                    {
                        Uri sensorId = observation.sensorId;
                        string twinId = sensorId.AbsolutePath.TrimStart('/').Replace(":","");

                        UpdateOperationsUtility uou = new UpdateOperationsUtility();
                        if (observation.numericValue.HasValue) {
                            uou.AppendReplaceOp("/hasValue", observation.numericValue.Value);
                        }
                        else if (observation.booleanValue.HasValue) {
                            uou.AppendReplaceOp("/hasValue", observation.booleanValue.Value);
                        }
                        else {
                            uou.AppendReplaceOp("/hasValue", observation.stringValue);
                        }

                        try {
                            log.LogInformation($"Updating twin '{twinId}' with operation '{uou.Serialize()}'");
                            await dtClient.UpdateDigitalTwinAsync(twinId, uou.Serialize());
                        }
                        catch (Exception ex) {
                            log.LogError(ex, ex.Message);
                        }
                    }
                }
            }
        }
    }
}