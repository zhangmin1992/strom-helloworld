package com.my.strom.shop;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.fastjson.JSONObject;

public class LogParseBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		//数据的格式是json字符串并不是普通string字符串
		String kafaMessage = tuple.getStringByField("kafaMessage");
		System.out.println("LogParseBolt 收到数据-----"+ kafaMessage);
		
		//字符串变成json对象，从json对象中获取productId
		JSONObject messageJSON = JSONObject.parseObject(kafaMessage);
		JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args"); 
		Long productId = uriArgsJSON.getLong("productId"); 
		
		if(productId != null) {
			collector.emit(new Values(productId)); 
			System.out.println("LogParseBolt 转发数据-----"+ productId);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("LogParse"));
	}

}
