using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using ServiceHubProcessor.Properties;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using Newtonsoft.Json;

namespace ServiceHubProcessor
{
    class SimpleEventProcessor : IEventProcessor
    {
        Stopwatch checkpointStopWatch;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine(string.Format("Processor Shuting Down.  Partition '{0}', Reason: '{1}'.", context.Lease.PartitionId, reason.ToString()));
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.WriteLine(string.Format("SimpleEventProcessor initialize.  Partition: '{0}', Offset: '{1}'", context.Lease.PartitionId, context.Lease.Offset));
            this.checkpointStopWatch = new Stopwatch();
            this.checkpointStopWatch.Start();
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (EventData eventData in messages)
            {
                try
                {
                    string data = Encoding.UTF8.GetString(eventData.GetBytes());

                    Console.WriteLine(string.Format("Message received.  Partition: '{0}', Data: '{1}'",
                        context.Lease.PartitionId, data));
                    // записываем в Table storage              

                    String s = Settings.Default.StorageConnectionString;//ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
                    Console.WriteLine(s);

                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(s);

                    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                    CloudTable table = tableClient.GetTableReference("Test1");

                    var obj = JsonConvert.DeserializeObject<dynamic>(data);

                    //var dict = obj.First.Children().Cast<JProperty>()
                    //            .ToDictionary(p => p.Name, p => p.Value);
                    //var a = (Int32)dict["active_value"];
                    //var r = (Int32)dict["reactive_value"];
                    //var c = (String)dict["uuid"];
                    //var d = (DateTime)dict["timestamp"];

                    CounterMeasurementEntity cme = new CounterMeasurementEntity(obj.uuid.ToString(), obj.timestamp.ToString());

                    cme.active_value = obj.active_value;
                    cme.reactive_value = obj.reactive_value;
                    cme.real_value = (Int32)Math.Sqrt(cme.active_value * cme.active_value + cme.reactive_value * cme.reactive_value);

                    // Create the TableOperation that inserts the customer entity.
                    TableOperation insertOperation = TableOperation.Insert(cme);

                    // Execute the insert operation.
                    table.Execute(insertOperation);
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(ex.Message);
                    Console.ResetColor();
                }

            }

            //Call checkpoint every 5 minutes, so that worker can resume processing from the 5 minutes back if it restarts.
            if (this.checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
                this.checkpointStopWatch.Restart();
            }
        }
    }

    public class CounterMeasurementEntity : TableEntity
    {
        public CounterMeasurementEntity(string uuid, string timestamp)
        {
            this.PartitionKey = uuid;
            this.RowKey = timestamp;
        }

        public CounterMeasurementEntity() { }

        public Int32 active_value { get; set; }

        public Int32 reactive_value { get; set; }

        public Int32 real_value { get; set; }
    }
}
