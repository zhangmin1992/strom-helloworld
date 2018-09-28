package com.my.strom.shop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * kafka消费的splout
 * @author yp-tc-m-7129
 *
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	//初始化的执行器
	private SpoutOutputCollector _collector;
	
	//将初始化数据放到阻塞队列中，然后再转发给下一个bolut
	private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);
	
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector _collector) {
		this._collector =_collector;
		//下面是一个模拟从kafka不断消费数据的过程
		startKafkaConsumer();
	}
	
	private void startKafkaConsumer() {
		//每次从主体消费1条数据
		String topic = "mytest2";
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        
        //zk部分
		Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("group.id", "strom-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (KafkaStream stream : streams) {
        	new Thread(new KafkaMessageProcessor(stream)).start();
//        	ConsumerIterator<byte[], byte[]> it = stream.iterator();
//	        while (it.hasNext()) {
//	        	String message = new String(it.next().message());
//	        	System.out.println("我收到从kafka的消息"+ message);
//	        	System.out.println("----"+queue.size());
//	        	try {
//					queue.put(message);
//					System.out.println("====="+queue.size());
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} 
//	        }
        }
	}
	
	private class KafkaMessageProcessor implements Runnable {

		@SuppressWarnings("rawtypes")
		private KafkaStream kafkaStream;
		
		@SuppressWarnings("rawtypes")
		public KafkaMessageProcessor(KafkaStream kafkaStream) {
			this.kafkaStream = kafkaStream;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
	        while (it.hasNext()) {
	        	String message = new String(it.next().message());
	        	System.out.println("我收到从kafka的消息"+ message);
	        	try {
					queue.put(message);
					System.out.println("====="+queue.size());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
	        }
		}
		
	}
	
	/**
	 * 不断的对初始化中收到的每条消息数据进行转发处理
	 */
	@Override
	public void nextTuple() {
		System.out.println("------队列大小"+ queue.size());
		if(queue.size() > 0) {
			try {
				String message = queue.take();
				System.out.println("我即将转发消息"+ message);
				_collector.emit(new Values(message)); 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else {
			Utils.sleep(800);
		}
	}

	/**
	 * 对转发的消息数据进行命名
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("kafaMessage"));
	}

}
