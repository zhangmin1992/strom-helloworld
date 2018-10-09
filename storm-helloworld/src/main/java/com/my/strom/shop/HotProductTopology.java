package com.my.strom.shop;

import org.apache.zookeeper.data.Stat;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class HotProductTopology {


	public static void main(String[] args) {
		
		/**
		 * .每次启动拓扑程序之前，把zk中的节点数据做一个删除,不能放在这里，导致taskid-list存放的只是最后一个taskid，之前的taskid都被这个初始化过程删除了
		 * 否则之前存在的节点在创建的时候回报错节点已存在
		 */
		ZooKeeperSession zkSession = ZooKeeperSession.getInstance();
		Stat resStat = zkSession.isExists("/taskid-list");
		if(resStat != null){
			zkSession.deleteNode("/taskid-list");
        }
		if(zkSession.isExists("/task-hot-product-list-5") != null){
			zkSession.deleteNode("/task-hot-product-list-5");
        }
		if(zkSession.isExists("/task-hot-product-list-6") != null){
			zkSession.deleteNode("/task-hot-product-list-6");
        }
		if(zkSession.isExists("/task-hot-product-list-4") != null){
			zkSession.deleteNode("/task-hot-product-list-4");
        }
		if(zkSession.isExists("/task-hot-product-list-3") != null){
			zkSession.deleteNode("/task-hot-product-list-3");
        }
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafaMessage", new AccessLogKafkaSpout(), 1);
		builder.setBolt("LogParseBolt", new LogParseBolt(), 2)
				.setNumTasks(2)
				.shuffleGrouping("kafaMessage");  
		builder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
				.setNumTasks(2)
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
