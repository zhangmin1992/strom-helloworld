package com.my.strom.shop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.utils.Utils;

import com.alibaba.fastjson.JSONObject;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * java版本的kafka  api 收发消息
 * @author yp-tc-m-7129
 *
 */
public class kafkaMain {

	public static void send() {
		//发送的消息和选择的主题
		String topic = "mytest";
		String message = "this is message 中文";

        //zk部分
		Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("group.id", "strom-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        /**
         * 需要这句话，否则传message会报错 java.lang.String cannot be cast to [B
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
        producer.send(data);
	}
	
	public static void receive() {
		// 一次从主题中获取一个数据
		String topic = "mytest";
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
        /**
         * 需要这句话，否则传message会报错 java.lang.String cannot be cast to [B
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        
        // 获取每次接收到的这个数据
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            consumer.commitOffsets();
            System.out.println("我收到消息"+ message);
        }
	}
	
	public static void send2() {
		//发送的消息和选择的主题
		String topic = "mytest2";
		
		//模拟用户请求到分发nginx，将请求日志转发到kafka，再从kafka消费日志统计最近访问的商品id的个数的数据模拟
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("user_name", "zhangmin");
		JSONObject jsonObject2 = new JSONObject();
		jsonObject2.put("productId", "56789");
		jsonObject2.put("shopid", "1");
		jsonObject.put("uri_args", jsonObject2);
		String message = jsonObject.toJSONString();
		
		
		JSONObject jsonObject3 = new JSONObject();
		jsonObject3.put("user_name", "haixing");
		JSONObject jsonObject4 = new JSONObject();
		jsonObject4.put("productId", "12345");
		jsonObject4.put("shopid", "2");
		jsonObject3.put("uri_args", jsonObject4);
		String message2 = jsonObject3.toJSONString();

        //zk部分
		Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("group.id", "strom-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        /**
         * 需要这句话，否则传message会报错 java.lang.String cannot be cast to [B
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        
        System.out.println("我准备发送消息了");
        
        //发送2条zhangmin的
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
        for(int i=0;i<2;i++) {
        	producer.send(data);
        }
        
        KeyedMessage<String, String> data2 = new KeyedMessage<String, String>(topic, message2);
        //发送3条海兴的
        for(int i=0;i<3;i++) {
        	producer.send(data2);
        }
        
	}
	
	public static void receive2() {
		// 每次从主体消费1条数据
		String topic = "mytest";
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);

		// zk部分
		Properties props = new Properties();
		props.put("zookeeper.connect", "127.0.0.1:2181");
		props.put("metadata.broker.list", "127.0.0.1:9092");
		props.put("group.id", "strom-cache-group");
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer
				.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		for (KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				String message = new String(it.next().message());
				System.out.println("我收到从kafka的消息" + message);
				JSONObject messageJSON = JSONObject.parseObject(message);
				JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args"); 
				Long productId = uriArgsJSON.getLong("productId"); 
				System.out.println("-----"+ productId);
			}
		}
	}
	
	public static void main(String[] args) {
		//send();
		//receive();
		/**
		 * 模拟从kafa消费统计商品访问量的数据的发送和处理
		 */
		send2();
		//receive2();
	}

}
