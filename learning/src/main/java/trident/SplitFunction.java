package trident;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
/**
 *
 * 功能描述: 单词切分操作
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/10 下午4:27
 */
public class SplitFunction implements Function {

	private static final long serialVersionUID = 5709475789218632706L;

	private int partitionIndex;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {

		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		String str = tuple.getStringByField("str");
		
		System.err.println("****** partitionIndex=" +this.partitionIndex +" , str=" + str);
		String[] words = str.split(" ");
		
		for(String word : words){
			collector.emit(new Values(word));
		}
		
	}

}
