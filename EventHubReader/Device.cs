using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace EventHubReader
{
    class Device : TableEntity
    {
        public int value { get; set; }

        public Device() { }
        public Device(int id)
        {
            this.PartitionKey = id.ToString();
            this.RowKey = System.Guid.NewGuid().ToString();
        }
    }
}
