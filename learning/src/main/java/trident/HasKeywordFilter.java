package trident;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @Auther: wuyue
 * @Date: 2018/8/10 16:07
 * @Description: 过滤器操作 主要是对数据进行处理
 */
public class HasKeywordFilter implements Filter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {

        String str = tridentTuple.getStringByField("str");
        //String key = tridentTuple.getStringByField("key");
        System.err.println(str);
        if(str.contains("flume")) {
            System.err.println("flume被找出来了： " + str);
            return true;
        }

//		String desc = tuple.getStringByField("describe");
//
//		System.err.println(desc);

        return false;
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
