package com.my.strom;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclamationTopology {

	public static void main(String[] args) throws Exception {
		// 构建一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
		
		//Spout的名字是word，对象是自定义的TestWordSpout类型，执行器有2个
		builder.setSpout("word", new TestWordSpout(), 2);
		//给这个bolt设置一个名字exclaim，一个bolt对象--ExclamationBolt，需要2执行器，分组策略负载均衡
		builder.setBolt("exclaim", new ExclamationBolt(), 2)
		       .shuffleGrouping("word");
		//给这个bolt设置一个名字exclaim，一个bolt对象--ExclamationBolt，需要2执行器，分组策略负载均衡
		builder.setBolt("print", new PrintBolt(), 1).shuffleGrouping("exclaim");

		Config conf = new Config();
		conf.setDebug(true);

		//说明是远程strom集群中执行
		if (args != null && args.length > 0) {
			System.out.println("我在远程执行");
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		} else {
			System.out.println("我在本地机器执行");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test3", conf, builder.createTopology());
			//指定时间后自动停止执行拓扑，时间太短会报错：
			/**
			 * org.apache.storm.shade.org.apache.zookeeper.server.ServerCnxn$EndOfStreamException: Unable to read additional data from client sessionid 
		    0x15c8a2872ac000f, likely client has closed socket
			 */
			Utils.sleep(8000);
			cluster.killTopology("test3");
			cluster.shutdown();
		}
	}
}
