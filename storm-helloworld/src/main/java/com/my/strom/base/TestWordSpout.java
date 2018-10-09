package com.my.strom.base;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestWordSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 9122758925572995126L;
	
	//spout的执行器
	private SpoutOutputCollector _collector;

	//类的初始化，参数会传入一个执行器，只需要这句话就行
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
       _collector = collector;
    }

	@Override
	public void nextTuple() {
	   //产生字符串部分，这里休息1毫秒
	   Utils.sleep(100);
       final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
       final Random rand = new Random();
       //从0-4之间选一个随机的单词
       final String word = words[rand.nextInt(words.length)];
       //把单词封装成Tuple,提交给执行器
       _collector.emit(new Values(word));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  //执行器发送数据的Field为word
		 declarer.declare(new Fields("word"));
	}

}
