using System;
using System.Globalization;
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
            await SendLog(message);
            
            message.Headers.FirstOrDefault(x => {
                    x.Fields.TryGetValue("Active", out string active);
                    return x.Name == "route-header" && active.ToLower() == "true";
                }).Fields.TryGetValue("Destination", out string queueName);
            if (queueName != null) {
                log.LogInformation($"Queue Name: {queueName}");
                var route = message.Headers.FirstOrDefault(x => {
                    x.Fields.TryGetValue("Active", out string active);
                    return x.Name == "route-header" && active.ToLower() == "true";
                });
                route.Fields["Active"] = "false";
                route.Fields.Add("SentTimestamp", DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
                var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
                current.Fields["Name"] = queueName;
                current.Fields["Timestamp"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                await QueueMessageAsync(queueName, message, log);
            }

        }

        private async Task SendLog(Message message)
        {
            var idHeader = message.Headers.FirstOrDefault(x => x.Name.Equals("id-header"));
            var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
            LogTimer logTimer = new LogTimer() {
                Id = Guid.Parse(idHeader.Fields["GUID"]),
                Queue = current.Fields["Name"],
                SentTimestamp = DateTime.ParseExact(current.Fields["Timestamp"], "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
                ReceiveTimestamp = DateTime.UtcNow
            };
            await using var client = new ServiceBusClient(Environment.GetEnvironmentVariable("rasputinServicebus"));
            ServiceBusSender sender = client.CreateSender("ms-logtimer");
            string queueMessage = JsonSerializer.Serialize(logTimer, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });            
            var messageBytes = Encoding.UTF8.GetBytes(queueMessage);
            ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            await sender.SendMessageAsync(messageObject, cancellationToken);
            await sender.CloseAsync();

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
            await sender.CloseAsync();
        }
    }
}
