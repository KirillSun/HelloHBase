using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

/// <summary>
/// This program shows the ability to create a SCP.NET topology using C# Spouts and Bolts.
/// For how to use SCP.NET, please refer to: http://go.microsoft.com/fwlink/?LinkID=525500&clcid=0x409
/// For more Storm samples, please refer to our GitHub repository: http://go.microsoft.com/fwlink/?LinkID=525495&clcid=0x409
/// </summary>

namespace StormSample1
{
	/// <summary>
	/// Implements the TopologyDescriptor interface to describe the topology in C#,
	/// and return a ITopologyBuilder instance. 
	/// This TopologyDescriptor is marked as Active
	/// </summary>
	[Active(true)]
	class HelloWorld : TopologyDescriptor
	{
		/// <summary>
		/// Use Topology Specification API to describe the topology
		/// </summary>
		/// <returns></returns>
		public ITopologyBuilder GetTopologyBuilder()
		{
			// Use TopologyBuilder to define a Non-Tx topology
			// And define each spouts/bolts one by one
			TopologyBuilder topologyBuilder = new TopologyBuilder(typeof(HelloWorld).Name + DateTime.Now.ToString("yyyyMMddHHmmss"));

			// Set a User customized config (Generator.config) for the Generator
			topologyBuilder.SetSpout(
				"generator",
				Generator.Get,
				new Dictionary<string, List<string>>()
				{
					{Constants.DEFAULT_STREAM_ID, new List<string>(){"sentence"}}
				},
				1,
				"Generator.config");

			topologyBuilder.SetBolt(
				"splitter",
				Splitter.Get,
				new Dictionary<string, List<string>>()
				{
					{Constants.DEFAULT_STREAM_ID, new List<string>(){"word", "firstLetterOfWord"}}
				},
				1).shuffleGrouping("generator");

			// Use scp-field-group from Splitter to Counter, 
			// and specify the second field in the Output schema of Splitter (Input schema of Counter) as the field grouping target
			// by passing the index array [1] (index start from 0) 
			topologyBuilder.SetBolt(
				"counter",
				Counter.Get,
				new Dictionary<string, List<string>>()
				{
					{Constants.DEFAULT_STREAM_ID, new List<string>(){"word", "count"}}
				},
				1).fieldsGrouping("splitter", new List<int>() { 1 });

			// Add topology config
			topologyBuilder.SetTopologyConfig(new Dictionary<string, string>()
			{
				{"topology.kryo.register","[\"[B\"]"}
			});

			return topologyBuilder;
		}
	}
}
