using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.DigitalTwins.Core.Serialization;
using Azure.Identity;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace JTHSmartSpace.AzureFunctions
{
    public static class IngestHomeAssistantEventTelemetry
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IngestHomeAssistantEventTelemetry")]
        public static async Task Run([EventHubTrigger("homeassistantingestion", Connection = "EVENTHUB_CONNECTION")] EventData[] events, ILogger log)
        {
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");

            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    //Authenticate with Digital Twins
                    ManagedIdentityCredential cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                    DigitalTwinsClient client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred, new DigitalTwinsClientOptions { 
                        Transport = new HttpClientTransport(httpClient) 
                    });

                    // Unpack message payload
                    string payload = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Extract device ID, device class (i.e., measurement type/quantity kind), and state from payload
                    // Home Assistant entity ids are named like <domain>.<identifier>, i.e., "sensor.MyTemperatureSensor"
                    // We keep only the latter half, and use that as ID for the sensor digital twins in ADT 
                    // Note that this implies that HomeAssistant sensors are given globally unique names. 
                    // TODO: support sensor names that are local to each HomeAssistant device.
                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(payload);
                    string homeAssistantEntityId = (string)deviceMessage["entity_id"];
                    string deviceId = homeAssistantEntityId.Split('.', 2)[1];
                    string homeAssistantDeviceClass = (string)deviceMessage["attributes"]["device_class"];
                    var sensorState = deviceMessage["state"];

                    // Create update operation for entity w/ value schema based on device class
                    UpdateOperationsUtility uou = new UpdateOperationsUtility();
                    switch (homeAssistantDeviceClass)
                    {
                        case "temperature":
                            uou.AppendReplaceOp("/hasValue", sensorState.Value<double>());
                            break;
                        case "illuminance":
                            uou.AppendReplaceOp("/hasValue", sensorState.Value<double>());
                            break;
                        case "motion":
                            bool motionValue = ((string)sensorState).Equals("on") ? true : false;
                            uou.AppendReplaceOp("/hasValue", motionValue);
                            break;
                        default:
                            uou.AppendReplaceOp("/hasValue", (string)sensorState);
                            break;
                    }
                    await client.UpdateDigitalTwinAsync(deviceId, uou.Serialize());

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
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
