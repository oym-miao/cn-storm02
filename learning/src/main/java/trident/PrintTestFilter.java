package trident;

import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;


/**
 *
 * 功能描述: 打印测试工具
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/10 下午4:19
 */
public class PrintTestFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6868959143417503368L;
	
	private int partitionIndex;

	public void prepare(Map conf, TridentOperationContext context) {
		this.partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {
	}
	/**
	 * 实现是否将Tuple保留在Stream中的逻辑
	 */
	public boolean isKeep(TridentTuple tuple) {
		
		List<Object> values = tuple.getValues();
		StringBuilder sbuilder = new StringBuilder("partitionIndex=" + this.partitionIndex +"---->");
		
		int i = 0;
		for(Object value : values){
			
			if(i == 0){
				sbuilder.append(value);
			}else{
				sbuilder.append("," + value);
			}
			
			i++;
		}
		
		System.err.println(sbuilder.toString());
		return true;
	}

}
