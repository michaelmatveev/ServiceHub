using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace EventHubReader
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        static void Main(string[] args)
        {
        }

        static int  partitionCount = Properties.Settings.Default.EVENTHUBPARTITIONCOUNT;
        JavaComponentConstructor constructor = JavaComponentConstructor.CreateFromClojureExpr(
            String.Format(@"(com.microsoft.eventhubs.spout.EventHubSpout. (com.microsoft.eventhubs.spout.EventHubSpoutConfig. " +
                @"""{0}"" ""{1}"" ""{2}"" ""{3}"" {4} ""{5}""))",
                Properties.Settings.Default.EVENTHUBPOLICYNAME,
                Properties.Settings.Default.EVENTHUBPOLICYKEY,
                Properties.Settings.Default.EVENTHUBNAMESPACE,
                Properties.Settings.Default.EVENTHUBNAME,
                partitionCount,
                "")); //Last value is the zookeeper connection string - leave empty

        public ITopologyBuilder GetTopologyBuilder()
        {
            TopologyBuilder topologyBuilder = new TopologyBuilder("EventHubReader");
            topologyBuilder.SetJavaSpout(
                "EventHubSpout",
                constructor,
                partitionCount);
            List<string> javaSerializerInfo = new List<string>() { "microsoft.scp.storm.multilang.CustomizedInteropJSONSerializer" };
            topologyBuilder.SetBolt(
                "Bolt",
                Bolt.Get,
                new Dictionary<string, List<string>>(),
                partitionCount,
                true).
                DeclareCustomizedJavaSerializer(javaSerializerInfo).
                shuffleGrouping("EventHubSpout");


            return topologyBuilder;
        }


        //public ITopologyBuilder GetTopologyBuilder()
        //{
        //    TopologyBuilder topologyBuilder = new TopologyBuilder("EventHubReader");
        //    topologyBuilder.SetSpout(
        //        "Spout",
        //        Spout.Get,
        //        new Dictionary<string, List<string>>()
        //        {
        //            {Constants.DEFAULT_STREAM_ID, new List<string>(){"count"}}
        //        },
        //        1);
        //    topologyBuilder.SetBolt(
        //        "Bolt",
        //        Bolt.Get,
        //        new Dictionary<string, List<string>>(),
        //        1).shuffleGrouping("Spout");

        //    return topologyBuilder;
        //}
    }
}

