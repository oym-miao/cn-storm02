package timewindow;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class MyTumplingWindow extends BaseWindowedBolt {
    private static final long serialVersionUID = -1169981820477739961L;
    @Override
    public void execute(TupleWindow inputWindow) {
        System.out.println("==========================================");
        //写业务逻辑了

        //拿到的是整个数据流
        List<Tuple> tuples1 = inputWindow.get();
        System.out.println("get()===>"+tuples1.toString());

        //前面的尾部
        List<Tuple> tuples2 = inputWindow.getExpired();
        System.out.println("getExpired()===>"+tuples2.toString());

        //后面的新增数据
        List<Tuple> tuples3 = inputWindow.getNew();
        System.out.println("getNew()"+"===>"+tuples3.toString());

        Double price = 0.0;
        List<Tuple> tuples = inputWindow.get();
        for(Tuple tuple : tuples){
             price += Double.parseDouble(tuple.getStringByField("price"));
        }
        Double avg_price = price/tuples.size();
        System.out.println(price+"===>"+avg_price);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }
}
