package orderproject;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 *
 * 功能描述: 订单数据解析、真是公司需要做数据处理、预清洗、等操作
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/11 下午9:11
 */
public class OrderDatasParseFunction implements Function {

	private static final long serialVersionUID = -8531306604648164614L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String orderRecord = tuple.getStringByField("str");
		
		if(orderRecord != null && !"".equals(orderRecord)){
			String[] orderDetails = orderRecord.replace("\"", "").split(" ");
			
			long timestamp = Long.valueOf(orderDetails[0]);
			
			Date date = new Date(timestamp);

			/**
			 * 为了业务更加精准，我们需要对时间进行年月日时分秒的处理，方便后续stream的处理
			 */

			DateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
			String yyyyMMddStr = yyyyMMdd.format(date);
			
			DateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
			String yyyyMMddHHStr = yyyyMMddHH.format(date);
			
			
			DateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
			String yyyyMMddHHmmStr = yyyyMMddHHmm.format(date);
			
			String consumer = orderDetails[1];
			String productName = orderDetails[2];
			double price = Double.valueOf(orderDetails[3]);
			String country = orderDetails[4];
			String province = orderDetails[5];
			String city = orderDetails[6];
			
			collector.emit(new Values(timestamp,yyyyMMddStr,yyyyMMddHHStr,yyyyMMddHHmmStr,
					consumer,productName,price, country,province,city));
		}

	}

}
