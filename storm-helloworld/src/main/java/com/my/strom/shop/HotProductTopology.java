package com.my.strom.shop;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class HotProductTopology {


	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafaMessage", new AccessLogKafkaSpout(), 3);
		builder.setBolt("LogParseBolt", new LogParseBolt(), 5)
				.setNumTasks(5)
				.shuffleGrouping("kafaMessage");  
		builder.setBolt("ProductCountBolt", new ProductCountBolt(), 8)
				.setNumTasks(10)
				.fieldsGrouping("LogParseBolt", new Fields("LogParse")); 
		
		Config config = new Config();
		
		if(args != null && args.length > 0) {
			config.setNumWorkers(3);  
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("HotProductTopology", config, builder.createTopology());  
			Utils.sleep(200000); 
			cluster.killTopology("HotProductTopology");
			cluster.shutdown();
		}
	}
}
