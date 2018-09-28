package com.my.strom;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt extends BaseRichBolt{

	//一个Bolt的初始化
	private OutputCollector _collector;
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector _collector) {
		this._collector=_collector;
	}

	//接收数据，交给执行器
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		System.out.println("------"+word);
		_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
	}

	
	//消息名定义
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("exclaim"));
	}

	
}
