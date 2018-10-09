package com.my.strom.base;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt {

	private OutputCollector _collector;
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector _collector) {
		this._collector=_collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("exclaim");
		System.out.println("我最终收到的数据是"+ word);
		_collector.ack(tuple);
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
