package com.my.strom.shop;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.data.Stat;

import storm.trident.util.LRUMap;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ProductCountBolt extends BaseRichBolt {

	//操作zk的工作类
	private ZooKeeperSession zkSession;
	
	//每个bolt的执行器
	private OutputCollector collector;
	
	//每一个task都会有一个不同的taskid
	private int taskid;

	/**
	 * 不是使用普通的HashMap而是LRUMap
	 */
	//private Map<String, Integer> countMap = new HashMap<String, Integer>();
	//写3表示存储最近访问的3次记录，这3次记录并不一定是访问次数最多的记录，很有可能访问次数最多的不再最近3次里面
	private LRUMap<Long, Long> countMap = new LRUMap<Long, Long>(1000);
	
	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		//1.正常消息的处理和变量的初始化
		this.collector = collector;
		this.zkSession = ZooKeeperSession.getInstance();
		this.taskid = context.getThisTaskId();
		System.out.println("this.taskid ----" + this.taskid);
		
		//2.每次启动拓扑程序之前，把zk中的节点数据做一个删除,不能放在这里，导致taskid-list存放的只是最后一个taskid，之前的taskid都被这个初始化过程删除了
		/*Stat resStat = zkSession.isExists("/taskid-list");
		if(resStat != null){
			zkSession.deleteNode("/taskid-list");
        }*/
				
		//3.统计topN
		new Thread(new ProductCountThread()).start();
		
		//4.初始化taskid
		initTaskId(context.getThisTaskId());
	}
	

	@Override
	public void execute(Tuple tuple) {
		long productId = tuple.getLongByField("LogParse");
		System.out.println("我最终接收到一条信息，准备统计数据 " + productId);
		
		Long count = countMap.get(productId);
		if(count == null){
			count = 0L;
		}
		count ++;
		countMap.put(productId, count);
		System.out.println("统计数据"+ JSONObject.toJSONString(countMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count"));
	}
	
	/**
	 * ProductCountBolt所有的task启动的时候， 都会将自己的taskid写到同一个node的值中
	 * @param taskid
	 */
	private void initTaskId(int taskid) {
		//1. 一直等待获取zk的一个taskid-list锁
		/**
		 * /taskid-list-lock地址前没有加/导致一直一直在试图创建这个节点但是失败，所以一直一直在等待
		 */
		zkSession.acquireDistributedLockForWait("/taskid-list-lock");
		
		//2.创建节点taskid-list
		zkSession.createNode("/taskid-list");  
		
		//3.获取taskidList节点的值，首先为空
		String taskidList = zkSession.getNodeData("/taskid-list");
		if(!"".equals(taskidList)) {
			//4.2 获取的taskidList不为空，就在后面加上,+当前的taskid
			taskidList += "," + taskid;
		} else {
			//4.1 taskidList值为当前的taskid
			taskidList += taskid;
		}
		//5.将taskidList的值写到节点中
		zkSession.setNodeData("/taskid-list", taskidList); 
		//【ProductCountBolt设置taskid list】taskidList=5,6,4,3如果重复是因为你没有删除上次拓扑跑的节点，这个的数量和ProductCountBolt这里设置的task的数量是一致的
		System.out.println("【ProductCountBolt设置taskid list】taskidList=" + taskidList);
		
		//6. 关闭分布式锁,直接删除，没有等待删除的情况
		zkSession.releaseDistributedLock("/taskid-list-lock");
	}
	
	/**
	 * 每隔一分钟算出商品访问次数排名前3的列表存储在topnProductList
	 */
	private class ProductCountThread implements Runnable {
		public void run() {
			List<Map.Entry<Long, Long>> topnProductList = new ArrayList<Map.Entry<Long, Long>>();
			List<Long> productidList = new ArrayList<Long>();

			while (true) {
				try {
					if(countMap.size() == 0) {
						Utils.sleep(100);
						continue;
					}
					
					topnProductList.clear();
					productidList.clear();
					int topn = 3;
					
					System.out.println("【ProductCountThread打印countMap的长度】size=" + countMap.size());
					
					for(Map.Entry<Long, Long> productCountEntry : countMap.entrySet()) {
						if(topnProductList.size() == 0) {
							topnProductList.add(productCountEntry);
						} else {
							// 比较大小，生成最热topn的算法有很多种
							// 但是我这里为了简化起见，不想引入过多的数据结构和算法的的东西
							// 很有可能还是会有漏洞，但是我已经反复推演了一下了，而且也画图分析过这个算法的运行流程了
							boolean bigger = false;
							
							for(int i = 0; i < topnProductList.size(); i++){
								Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);
								
								if(productCountEntry.getValue() > topnProductCountEntry.getValue()) {
									int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
									for(int j = lastIndex; j >= i; j--) {
										if(j + 1 == topnProductList.size()) {
											topnProductList.add(null);
										}
										topnProductList.set(j + 1, topnProductList.get(j));  
									}
									topnProductList.set(i, productCountEntry);
									bigger = true;
									break;
								}
							}
							
							if(!bigger) {
								if(topnProductList.size() < topn) {
									topnProductList.add(productCountEntry);
								}
							}
						}
					}
					
					// 上面的算法可以获取到一个topn list，从topn list里面获取productidList
					for(Map.Entry<Long, Long> topnProductEntry : topnProductList) {
						productidList.add(topnProductEntry.getKey());
					}
					
					String topnProductListJSON = JSONArray.toJSONString(productidList);
					System.out.println("taskid=" + taskid + "计算出一份热门数据，准备插入zk productidList"+ topnProductListJSON);
					
					zkSession.createNode("/task-hot-product-list-" + taskid); 
					zkSession.setNodeData("/task-hot-product-list-" + taskid, topnProductListJSON);
				    System.out.println("【ProductCountThread计算出一份top3热门商品列表】zk path=" + ("/task-hot-product-list-" + taskid) + ", topnProductListJSON=" + topnProductListJSON);  
					
					Utils.sleep(5000);
				}catch(Exception e) {
					System.out.println("发生未知异常" + e.getMessage());
				}
			}
		}
	}
}
