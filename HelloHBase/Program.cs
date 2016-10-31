using Microsoft.HBase.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using org.apache.hadoop.hbase.rest.protobuf.generated;

namespace HelloHBase
{
	class Program
	{
		static void Main(string[] args)
		{
			try
			{
				Task theTask = MainAsync(args);
				theTask.Wait();
			}
			catch (Exception ex)
			{
				
				throw;
			}
		}

		static async Task MainAsync(string[] args)
		{
			try
			{
				string clusterURL = "https://myClusterName.azurehdinsight.net";
				string hadoopUsername = "admin";
				string hadoopUserPassword = "	pwd123!@#QWE";

				string hbaseTableName = "sampleHbaseTable";

				// Create a new instance of an HBase client.
				ClusterCredentials creds = new ClusterCredentials(new Uri(clusterURL), hadoopUsername, hadoopUserPassword);
				HBaseClient hbaseClient = new HBaseClient(creds);

				// Retrieve the cluster version.
				org.apache.hadoop.hbase.rest.protobuf.generated.Version version = await hbaseClient.GetVersionAsync();
				Console.WriteLine("The HBase cluster version is " + version);

				// Create a new HBase table.
				TableSchema testTableSchema = new TableSchema();
				testTableSchema.name = hbaseTableName;
				testTableSchema.columns.Add(new ColumnSchema() { name = "d" });
				testTableSchema.columns.Add(new ColumnSchema() { name = "f" });
				hbaseClient.CreateTableAsync(testTableSchema).Wait();

				// Insert data into the HBase table.
				string testKey = "content";
				string testValue = "the force is strong in this column";
				CellSet cellSet = new CellSet();
				CellSet.Row cellSetRow = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey) };
				cellSet.rows.Add(cellSetRow);

				Cell value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
				cellSetRow.values.Add(value);
				hbaseClient.StoreCellsAsync(hbaseTableName, cellSet).Wait();

				// Retrieve a cell by its key.
				cellSet = await hbaseClient.GetCellsAsync(hbaseTableName, testKey);
				Console.WriteLine("The data with the key '" + testKey + "' is: " + Encoding.UTF8.GetString(cellSet.rows[0].values[0].data));
				// with the previous insert, it should yield: "the force is strong in this column"

				//Scan over rows in a table. Assume the table has integer keys and you want data between keys 25 and 35.
				Scanner scanSettings = new Scanner()
				{
					batch = 10,
					startRow = BitConverter.GetBytes(25),
					endRow = BitConverter.GetBytes(35)
				};

				ScannerInformation scannerInfo = await hbaseClient.CreateScannerAsync(hbaseTableName, scanSettings, null);
				CellSet next = null;
				Console.WriteLine("Scan results");

				while ((next = await hbaseClient.ScannerGetNextAsync(scannerInfo, null)) != null)
				{
					foreach (CellSet.Row row in next.rows)
					{
						Console.WriteLine(row.key + " : " + Encoding.UTF8.GetString(row.values[0].data));
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
			}

			Console.WriteLine("Press ENTER to continue ...");
			Console.ReadLine();
		}
	}
}
