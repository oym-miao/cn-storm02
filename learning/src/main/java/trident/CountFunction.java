package trident;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 功能描述: 单词统计
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/10 下午4:39
 */
public class CountFunction implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8179050548485979154L;
	
	private Map<String,Long> wordcounts;
	
	private int partitionIndex;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		wordcounts = new HashMap<String,Long>();
		
		partitionIndex = context.getPartitionIndex();
		System.err.println("countFunction的分区编号：" + partitionIndex);
	}

	@Override
	public void cleanup() {
	}
	/**
	 *
	 * 功能描述: 单词累加功能实现
	 *
	 * @param:
	 * @return:
	 * @auther: wuyue
	 * @date: 2018/8/10 下午4:41
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String word = tuple.getStringByField("word");
		Long counts = 0L;
		if(wordcounts.containsKey(word)){
			counts = wordcounts.get(word);
		}
		counts += 1;
		wordcounts.put(word, counts);
		for(String w: wordcounts.keySet()){
			System.err.println("partitionIndex="+ partitionIndex + "--->word=" + w + ",count=" + wordcounts.get(w));
		}
	}

}
