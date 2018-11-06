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

        //拿到的是整个数据流
        List<Tuple> tuples1 = inputWindow.get();
        System.out.println("get()===>"+tuples1.toString());

        //前面的尾部
        List<Tuple> tuples2 = inputWindow.getExpired();
        System.out.println("getExpired()===>"+tuples2.toString());

        //后面的新增数据
        List<Tuple> tuples3 = inputWindow.getNew();
        System.out.println("getNew()"+"===>"+tuples3.toString());

        System.out.println("近2个订单金额为：==》"+price);
    }
}
