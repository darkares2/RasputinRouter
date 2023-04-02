using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Rasputin.Router
{
    public class QueueTriggerRouter
    {
        [FunctionName("QueueTriggerRouter")]
        public async Task RunAsync([ServiceBusTrigger("api-router", Connection = "rasputinServicebus")]string myQueueItem, ILogger log)
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
            await using var client = new ServiceBusClient(Environment.GetEnvironmentVariable("rasputinServicebus"));
            ServiceBusSender sender = client.CreateSender(queueName);

            string queueMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });            
            var messageBytes = Encoding.UTF8.GetBytes(queueMessage);
            ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            await sender.SendMessageAsync(messageObject, cancellationToken);
        }
    }
}
