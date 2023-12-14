
using Azure.Identity;
using Azure.Messaging.ServiceBus;

namespace ServiceBusDemo.Queue
{
    public static class QueueSender
    {
        private static string _namespaceConnectionString = "your service bus connection string";
        private static string _queueName = "your queue name";

        private static int _numberOfMessages = 3;

        public static async Task SendMessages()
        {
            var clientOptions = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };

            var client = new ServiceBusClient(_namespaceConnectionString, clientOptions);
            var sender = client.CreateSender(_queueName);

            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            for (int i = 1; i <= _numberOfMessages; i++)
            {
                // This Try is used to check if the size of the batch with the new message is too large, if so, it will throw an exception;
                if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Message {i}")))
                {
                    throw new Exception($"The message {i} is too large to fit in the batch.");
                }
            }

            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"A batch of {_numberOfMessages} messages has been published to the queue.");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        public static async Task SendMessagesOneByOne()
        {
            var clientOptions = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };

            var serviceBusClient = new ServiceBusClient(_namespaceConnectionString, clientOptions);

            var serviceBusSender = serviceBusClient.CreateSender(_queueName);

            try
            {
                for (int i = 1; i <= _numberOfMessages; i++)
                {
                    await serviceBusSender.SendMessageAsync(new ServiceBusMessage($"Message {i}"));
                    Console.WriteLine($"Sent: Message {i}");
                }
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up;
                await serviceBusSender.DisposeAsync();
                await serviceBusClient.DisposeAsync();
            }
        }
    }
}
