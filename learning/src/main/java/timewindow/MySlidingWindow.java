package timewindow;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.Window;

import java.util.List;

public class MySlidingWindow extends BaseWindowedBolt {


    private static final long serialVersionUID = 769845293978493155L;
    Double price=0.0;

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> newTuple = inputWindow.getNew();
        List<Tuple> expiredTuple = inputWindow.getExpired();


        for(Tuple tuple : newTuple){
            price += Double.parseDouble(tuple.getStringByField("price"));
        }

        for(Tuple tuple : expiredTuple){
            price -= Double.parseDouble(tuple.getStringByField("price"));
        }
        System.out.println("近2个订单金额为：==》"+price);
    }
}
