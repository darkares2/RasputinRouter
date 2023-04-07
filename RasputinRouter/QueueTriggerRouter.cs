using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            log.LogInformation($"api-router triggered: {myQueueItem}");
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DateTime receivedMessageTime = DateTime.UtcNow;
            var message = JsonSerializer.Deserialize<Message>(myQueueItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var logMessage = new Message();
            try {
                List<MessageHeader> headers = new List<MessageHeader>();
                headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", message.Headers.FirstOrDefault(x => x.Name.Equals("id-header")).Fields["GUID"] } } });
                headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header")).Fields["Name"] }, { "Timestamp", message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header")).Fields["Timestamp"] } } });
                logMessage.Headers = headers.ToArray();

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
                    try {
                        await QueueMessageAsync(queueName, message, log);
                    } catch {
                        stopwatch.Stop();
                        await SendLog(logMessage, receivedMessageTime, stopwatch.ElapsedMilliseconds);
                        throw;
                    }
                }
                stopwatch.Stop();
                await SendLog(logMessage, receivedMessageTime, stopwatch.ElapsedMilliseconds);
            } catch(Exception ex) {
                log.LogError("Processing failed", ex);
                var current = logMessage.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
                current.Fields["Name"] = current.Fields["Name"] + $"-Error (Router): {ex.Message}";
                stopwatch.Stop();
                await SendLog(logMessage, receivedMessageTime, stopwatch.ElapsedMilliseconds);
            }

        }

        private static async Task SendLog(Message message, DateTime receivedMessageTime, long elapsedMilliseconds)
        {
            var idHeader = message.Headers.FirstOrDefault(x => x.Name.Equals("id-header"));
            var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
            LogTimer logTimer = new LogTimer() {
                Id = Guid.Parse(idHeader.Fields["GUID"]),
                Queue = current.Fields["Name"],
                SentTimestamp = DateTime.ParseExact(current.Fields["Timestamp"], "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
                ReceiveTimestamp = receivedMessageTime,
                ProcesMs = elapsedMilliseconds
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
