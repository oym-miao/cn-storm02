package orderproject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * 功能描述: 模拟订单数据生成
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/10 下午11:22
 */
public class OrderTestData {

	private static final String[] CONSUMERS = { "张三", "莉丝", "Tom", "jesson", "陈伟霆",
			"孙燕姿", "陈诚", "张艺谋", "成武","莉莉", "katto" };

	private static final String[] PRODUCT_NAMES = { "华为手机", "苹果平板", "Mac", "iPhone", "小米手环",
			"海尔电器", "智能家具", "大数据书籍" };

	private static final Map<String, Double> PRODUCT_PRICE = new HashMap<>();
	static {
		PRODUCT_PRICE.put("华为手机", 234.4);
		PRODUCT_PRICE.put("苹果平板", 367.78);
		PRODUCT_PRICE.put("Mac", 3456.12);
		PRODUCT_PRICE.put("iPhone", 6932.81);
		PRODUCT_PRICE.put("小米手环", 134.76);
		PRODUCT_PRICE.put("海尔电器", 160.32);
		PRODUCT_PRICE.put("智能家具", 2390.81);
		PRODUCT_PRICE.put("大数据书籍", 36.72);
	}

	private static final String[] ADDRESSES = { "中国,安徽,合肥", "中国,上海,杨浦区", "中国,浙江,宁波",
			"中国,湖北,武汉", "中国,江苏,扬州", "中国,北京,通州", "中国,香港,香港" };

	/**----- "timestamp" "consumer" "productName" "price" "country" "province" "city" **------/

	 /**
	 * 模拟生成订单消费记录
	 * 
	 * @return
	 */
	public static String generateOrderRecord() {
		long timestamp = System.currentTimeMillis();
		StringBuilder stringBuilder = new StringBuilder("\""+timestamp + "\"");

		Random r = new Random();
		String consumer = CONSUMERS[r.nextInt(CONSUMERS.length)];
		stringBuilder.append(" \"" + consumer + "\"");
		String productName = PRODUCT_NAMES[r.nextInt(PRODUCT_NAMES.length)];
		stringBuilder.append(" \"" + productName + "\"");
		double price = PRODUCT_PRICE.get(productName);
		stringBuilder.append(" \"" + price + "\"");
		String address = ADDRESSES[r.nextInt(ADDRESSES.length)];
		String[] addrInfos = address.split(",");
		stringBuilder.append(" \"" + addrInfos[0]  + "\"");
		stringBuilder.append(" \"" + addrInfos[1] + "\"");
		stringBuilder.append(" \"" + addrInfos[2] + "\"");

		return stringBuilder.toString();
	}


	/**
	 * 创建生产者 在服务器上进行处理
	 * @param args
	 */
	public static void main(String[] args) {
		MyKafkaProducer kafkaProducer = new MyKafkaProducer();
		Producer<String,String> producer = kafkaProducer.getKafkaProducer("106.15.183.124:9092");
		for (; ; ) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			String msgKey = System.currentTimeMillis()+ "";
			String msg = OrderTestData.generateOrderRecord();
			// 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
			KeyedMessage<String, String> data = kafkaProducer.getKeyedMessage("test", msgKey, msg);
			kafkaProducer.sendMassage(producer, data);
		}
	}
}
