package com.my.strom.base;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单词计数拓扑
 * 
 * 我认识很多java工程师，都是会一些大数据的技术的，不会太精通，没有那么多的时间去研究
 * storm的课程，我就只是讲到，最基本的开发，就够了，java开发广告计费系统，大量的流量的引入和接入，就是用storm做得
 * 用storm，主要是用它的成熟的稳定的易于扩容的分布式系统的特性
 * java工程师，来说，做一些简单的storm开发，掌握到这个程度差不多就够了
 * 
 * @author Administrator
 *
 */
public class WordCountTopology {
	
	/**
	 * 内部类 spout
	 * spout，继承一个基类，实现接口，这个里面主要是负责从数据源获取数据
	 * 我们这里作为一个简化，就不从外部的数据源去获取数据了，只是自己内部不断发射一些句子
	 */
	public static class RandomSentenceSpout extends BaseRichSpout {

		private static final long serialVersionUID = 3699352201538354417L;
		
		//执行器
		private SpoutOutputCollector collector;
		
		//产生随机字符串的工具，写在这里比nextTuple中少创建好多Random对象
		private Random random;
		
		/**
		 * open方法，open方法，是对spout进行初始化的
		 * 比如说，创建一个线程池，或者创建一个数据库连接池，或者构造一个httpclient，构造一个随机数字工具
		 */
		@SuppressWarnings("rawtypes")
		public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
			// 在open方法初始化的时候，会传入进来一个东西，叫做SpoutOutputCollector，这个SpoutOutputCollector就是用来发射数据出去的
			this.collector = collector;
			// 构造一个随机数生产对象
			this.random = new Random();
		}
		
		/**
		 * nextTuple方法
		 * 这个spout类，之前说过，最终会运行在task中，某个worker进程的某个executor线程内部的某个task中
		 * 那个task会负责去不断的无限循环调用nextTuple()方法
		 * 只要的话呢，无限循环调用，可以不断发射最新的数据出去，形成一个数据流
		 * 
		 */
		public void nextTuple() {
			//不要每毫秒就产生数据流，休息会你再产生
			Utils.sleep(200); 
			String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
					"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};
			String sentence = sentences[random.nextInt(sentences.length)];
			System.out.println("spoult 准备【发射句子】sentence=" + sentence);  
			// 这个values，你可以认为就是构建一个tuple，tuple是最小的数据单位，无限个tuple组成的流就是一个stream
			collector.emit(new Values(sentence)); 
		}

		/**
		 * declareOutputFielfs这个方法
		 * 很重要，这个方法是定义一个你发射出去的每个tuple中的每个field的名称是什么
		 */
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sentence"));   
		}
		
	}

	/**
	 * 写一个bolt，直接继承一个BaseRichBolt基类
	 * 实现里面的所有的方法即可，每个bolt代码，同样是发送到worker中的某个executor的task里面去运行
	 */
	public static class SplitSentence extends BaseRichBolt {
		
		private static final long serialVersionUID = 6604009953652729483L;
		
		private OutputCollector collector;
		
		/**
		 * 对于bolt的初始化
		 * OutputCollector，这个也是Bolt的这个tuple的发射器
		 */
		@SuppressWarnings("rawtypes")
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}
		
		/**
		 * execute方法
		 * 就是说，每次接收到一条数据后，就会交给这个executor方法来执行
		 */
		public void execute(Tuple tuple) {
			//得到来自上游的数据流名称为sentence的数据
			String sentence = tuple.getStringByField("sentence"); 
			//提交的数据流不是整个话，而是话中的每个单词
			String[] words = sentence.split(" "); 
			for(String word : words) {
				collector.emit(new Values(word)); 
			}
		}

		/**
		 * 定义发射出去的tuple，每个field的名称
		 */
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));   
		}
		
	}
	
	public static class WordCount extends BaseRichBolt {

		private static final long serialVersionUID = 7208077706057284643L;

		private OutputCollector collector;
		
		private Map<String, Long> wordCounts = new HashMap<String, Long>();
		
		//初始化
		@SuppressWarnings("rawtypes")
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}
		
		//接收数据
		public void execute(Tuple tuple) {
			//得到上游发过来的数据流名称为word的单词数据，并计算出现次数
			String word = tuple.getStringByField("word");
			Long count = wordCounts.get(word);
			if(count == null) {
				count = 0L;
			}
			count++;
			wordCounts.put(word, count);
			System.out.println("【单词计数】" + word + "出现的次数是" + count);  
			
			collector.emit(new Values(word, count));
			collector.ack(tuple);
		}

		//因为他没有下游了，所以这里不写名称也是可以的
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//declarer.declare(new Fields("word", "count"));    
		}
		
	}
	
	public static void main(String[] args) {
		
		// 在main方法中，会去将spout和bolts组合起来，构建成一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
	
		/**
		 * 关于名称和参数的解释，
		 */
		// 这里的第一个参数的意思，就是给这个spout设置一个名字
		// 第二个参数的意思，就是创建一个spout的对象
		// 第三个参数的意思，就是设置spout的executor有几个
		builder.setSpout("sentence", new RandomSentenceSpout(), 2);
		//给这个bolt设置一个名字，一个bolt对象，需要几个执行器，需要任务数量，分组策略
		builder.setBolt("wordName", new SplitSentence(), 5)
				.setNumTasks(10)
				.shuffleGrouping("sentence");
		// 这个很重要，就是说，相同的单词，从SplitSentence发射出来时，一定会进入到下游的指定的同一个task中
		// 只有这样子，才能准确的统计出每个单词的数量--fieldsGrouping，比如你有个单词，hello，5个hello，全都进入一个task
		builder.setBolt("WordCount", new WordCount(), 10)
				.setNumTasks(20)
				.fieldsGrouping("wordName", new Fields("word"));  
		
		Config config = new Config();
	
		// 说明是在命令行执行，打算提交到storm集群上去，远程运行
		if(args != null && args.length > 0) {
			//strom启动几个wroker去执行拓扑
			config.setNumWorkers(3);  
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());  
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// 说明是在eclipse里面，本地运行
			config.setMaxTaskParallelism(20);  
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("WordCountTopology", config, builder.createTopology());  
			
			//休眠1分钟是为了看到打印的日志
			Utils.sleep(5000); 
			cluster.killTopology("WordCountTopology");
			cluster.shutdown();
		}
	}
	
}
