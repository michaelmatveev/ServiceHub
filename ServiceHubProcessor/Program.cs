using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Threading.Tasks;

namespace ServiceHubProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            string eventHubConnectionString = "Endpoint=sb://cloud-counters.servicebus.windows.net/;SharedAccessKeyName=ReceiveRule;SharedAccessKey=xHvbo7AYY3LkGMgf0/ZH18VfTwzc99MdskOnZrwUdsU=";
            string eventHubName = "counters-event-hub";
            string storageAccountName = "cloudcounters";
            string storageAccountKey = "Z6yA1b5m3rLqdmcNZekqTdRBVVNBKZ10ST/V0r3QXsGNUVy0Xiwxh7WmUVJoiapLwIRdNEwC1SkOCgW2EbZhTQ==";
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                        storageAccountName, storageAccountKey);

            string eventProcessorHostName = Guid.NewGuid().ToString();
            EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName, EventHubConsumerGroup.DefaultGroupName, eventHubConnectionString, storageConnectionString);
            eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>().Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker.");
            Console.ReadLine();
        }
    }
}
