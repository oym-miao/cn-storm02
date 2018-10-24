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
