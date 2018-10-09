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
		jsonObject2.put("productId", "11111");
		jsonObject2.put("shopid", "1");
		jsonObject.put("uri_args", jsonObject2);
		String message = jsonObject.toJSONString();
		
		
		JSONObject jsonObject3 = new JSONObject();
		jsonObject3.put("user_name", "haixing");
		JSONObject jsonObject4 = new JSONObject();
		jsonObject4.put("productId", "22222");
		jsonObject4.put("shopid", "2");
		jsonObject3.put("uri_args", jsonObject4);
		String message2 = jsonObject3.toJSONString();
		
		JSONObject jsonObject5 = new JSONObject();
		jsonObject5.put("user_name", "qita");
		JSONObject jsonObject6 = new JSONObject();
		jsonObject6.put("productId", "33333");
		jsonObject6.put("shopid", "1");
		jsonObject5.put("uri_args", jsonObject6);
		String message3 = jsonObject5.toJSONString();
		
		
		JSONObject jsonObject7 = new JSONObject();
		jsonObject7.put("user_name", "zuishao");
		JSONObject jsonObject8 = new JSONObject();
		jsonObject8.put("productId", "44444");
		jsonObject8.put("shopid", "2");
		jsonObject7.put("uri_args", jsonObject8);
		String message4 = jsonObject7.toJSONString();
		
		
		JSONObject jsonObject9 = new JSONObject();
		jsonObject9.put("user_name", "zhangmin");
		JSONObject jsonObject10 = new JSONObject();
		jsonObject10.put("productId", "55555");
		jsonObject10.put("shopid", "1");
		jsonObject9.put("uri_args", jsonObject10);
		String message5 = jsonObject9.toJSONString();
		
		JSONObject jsonObject11 = new JSONObject();
		jsonObject11.put("user_name", "zhangmin");
		JSONObject jsonObject12 = new JSONObject();
		jsonObject12.put("productId", "66666");
		jsonObject12.put("shopid", "1");
		jsonObject11.put("uri_args", jsonObject12);
		String message6 = jsonObject11.toJSONString();
		
		JSONObject jsonObject13 = new JSONObject();
		jsonObject13.put("user_name", "zhangmin");
		JSONObject jsonObject14 = new JSONObject();
		jsonObject14.put("productId", "77777");
		jsonObject14.put("shopid", "1");
		jsonObject13.put("uri_args", jsonObject14);
		String message7 = jsonObject13.toJSONString();
		
		JSONObject jsonObject15 = new JSONObject();
		jsonObject15.put("user_name", "zhangmin");
		JSONObject jsonObject16 = new JSONObject();
		jsonObject16.put("productId", "88888");
		jsonObject16.put("shopid", "1");
		jsonObject15.put("uri_args", jsonObject16);
		String message8 = jsonObject15.toJSONString();

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
        
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
        for(int i=0;i<1;i++) {
        	producer.send(data);
        }
        
        KeyedMessage<String, String> data2 = new KeyedMessage<String, String>(topic, message2);
        for(int i=0;i<2;i++) {
        	producer.send(data2);
        }
        
        KeyedMessage<String, String> data3 = new KeyedMessage<String, String>(topic, message3);
        for(int i=0;i<3;i++) {
        	producer.send(data3);
        }
        
        KeyedMessage<String, String> data4 = new KeyedMessage<String, String>(topic, message4);
        for(int i=0;i<4;i++) {
        	producer.send(data4);
        }
        
        KeyedMessage<String, String> data5 = new KeyedMessage<String, String>(topic, message5);
        for(int i=0;i<5;i++) {
        	producer.send(data5);
        }

        KeyedMessage<String, String> data6 = new KeyedMessage<String, String>(topic, message6);
        for(int i=0;i<6;i++) {
        	producer.send(data6);
        }
        
        KeyedMessage<String, String> data7 = new KeyedMessage<String, String>(topic, message7);
        for(int i=0;i<7;i++) {
        	producer.send(data7);
        }
        
        KeyedMessage<String, String> data8 = new KeyedMessage<String, String>(topic, message8);
        for(int i=0;i<8;i++) {
        	producer.send(data8);
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
