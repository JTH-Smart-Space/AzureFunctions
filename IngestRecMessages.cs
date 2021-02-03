// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using System.Collections.Generic;
using System.Net.Http;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Core.Pipeline;
using System.Text;
using Newtonsoft.Json.Linq;
using Azure.DigitalTwins.Core.Serialization;
using Newtonsoft.Json;
using RealEstateCore;
using System.Linq;

namespace JTHSmartSpace.AzureFunctions
{
    public static class IngestRecMessages
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IngestRecMessages")]
        public static async void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");

            var exceptions = new List<Exception>();

            //Authenticate with Digital Twins
            ManagedIdentityCredential cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
            DigitalTwinsClient client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred, new DigitalTwinsClientOptions
            {
                Transport = new HttpClientTransport(httpClient)
            });

            // Unpack message payload
            JObject payload = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
            string recMessageBody = (string)payload["body"];

            RecEdgeMessage recMessage = JsonConvert.DeserializeObject<RecEdgeMessage>(recMessageBody);
            log.LogInformation(recMessage.ToString());

            foreach (Observation observation in recMessage.observations)
            {
                log.LogInformation($"Sensor: {observation.sensorId.AbsoluteUri}\tQuantityKind: {observation.quantityKind.AbsoluteUri}");

                try
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

                    await client.UpdateDigitalTwinAsync(twinId, uou.Serialize());
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
            {
                throw new AggregateException(exceptions);
            }

            if (exceptions.Count == 1)
            {
                throw exceptions.Single();
            }
        }
    }
}