package orderproject;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * 功能描述: kafka生产者
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/11 上午12:22
 */
public class MyKafkaProducer {
	
	/**
	 * 获取Kafka
	 * @param brokerList
	 * @return
	 */
	public Producer<String,String> getKafkaProducer(String brokerList){
		// 设置配置属性
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "orderproject.MyKafkaPartitioner");
		props.put("request.required.acks", "1");
		props.put("request.timeout.ms","90000");
		ProducerConfig config = new ProducerConfig(props);

		// 创建producer
		Producer<String, String> producer = new Producer<String, String>(config);
		
		return producer;
	}
	
	/**
	 * 关闭kafka生产者
	 * @param producer
	 */
	public void close(Producer<String,String> producer){
		producer.close();
	}
	
	/**
	 * 发送数据到Kafka上
	 * @param producer
	 * @param data
	 */
	public void sendMassage(Producer<String,String> producer,KeyedMessage<String, String> data){
		producer.send(data);
	}
	
	/**
	 * 组装消息
	 * @param topic
	 * @param msgKey
	 * @param msgContent
	 * @return
	 */
	public KeyedMessage<String,String> 
		getKeyedMessage(String topic,String msgKey,String msgContent){
		KeyedMessage<String,String> data = new KeyedMessage<String,String>(topic,msgKey,msgContent);
		return data;
	}
}
