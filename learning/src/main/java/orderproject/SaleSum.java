package orderproject;

import java.util.Map;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;


/**
 *
 * 功能描述: 局部统计
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/11 下午9:43
 */
public class SaleSum implements Aggregator<SaleSumState> {
	
	private Logger logger  = org.slf4j.LoggerFactory.getLogger(SaleSum.class);

	private static final long serialVersionUID = -6879728480425771684L;

	private int partitionIndex ;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		
		partitionIndex = context.getPartitionIndex();
		logger.debug("partitionIndex=" + partitionIndex);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public SaleSumState init(Object batchId, TridentCollector collector) {
		return new SaleSumState();
	}

	@Override
	public void aggregate(SaleSumState val, TridentTuple tuple, TridentCollector collector) {
		
		double oldSaleSum = val.saleSum;
		 
		double price = tuple.getDoubleByField("price");
		
		double newSaleSum = oldSaleSum + price ;
		
		val.saleSum = newSaleSum;
		
	}

	@Override
	public void complete(SaleSumState val, TridentCollector collector) {
		System.err.println("SaleSum---> partitionIndex=" + this.partitionIndex + ",saleSum=" + val.saleSum);
		collector.emit(new Values(val.saleSum));
		
	}


}
