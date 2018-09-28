package com.my.strom.shop;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import storm.trident.util.LRUMap;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ProductCountBolt extends BaseRichBolt {

	private OutputCollector collector;

	/**
	 * 不是使用普通的HashMap而是LRUMap
	 */
	//private Map<String, Integer> countMap = new HashMap<String, Integer>();
	private LRUMap<String, Long> countMap = new LRUMap<String, Long>(3);
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		long productId = tuple.getLongByField("LogParse");
		String id = String.valueOf(productId);
		Long count = countMap.get(id);
		if(count == null){
			count = 0L;
		}
		count ++;
		countMap.put(id, count);
		System.out.println("统计数据"+ JSONObject.toJSONString(countMap));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count"));
	}

}
