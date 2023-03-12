using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Rasputin.Router
{
    public class QueueTriggerRouter
    {
        [FunctionName("QueueTriggerRouter")]
        public async Task RunAsync([QueueTrigger("api-router", Connection = "rasputinstorageaccount_STORAGE")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            var message = JsonSerializer.Deserialize<Message>(myQueueItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            message.Headers.FirstOrDefault(x => {
                    x.Fields.TryGetValue("Active", out string active);
                    return x.Name == "route-header" && active.ToLower() == "true";
                }).Fields.TryGetValue("Destination", out string queueName);
            if (queueName != null) {
                log.LogInformation($"Queue Name: {queueName}");
                message.Headers.FirstOrDefault(x => {
                    x.Fields.TryGetValue("Active", out string active);
                    return x.Name == "route-header" && active.ToLower() == "true";
                }).Fields["Active"] = "false";
                await QueueMessageAsync(queueName, message, log);
            }

        }

        private async Task QueueMessageAsync(string queueName, Message message, ILogger log)
        {
            // Get a reference to the queue
            var str = Environment.GetEnvironmentVariable("rasputinstorageaccount_STORAGE");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(str);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference(queueName);

            // Create a new message and add it to the queue
            CloudQueueMessage queueMessage = new CloudQueueMessage(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
                );
            await queue.AddMessageAsync(queueMessage);
        }
    }
}
