package trident;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 *
 * 功能描述: 通过trident来实现词频统计功能
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/10 下午4:03
 */
public class WordCountTrident {

    private static final String SPOUT_ID = "testSpout";

    public static void main(String[] args) {

        @SuppressWarnings("unchecked")
        FixedBatchSpout testSpout =
                new FixedBatchSpout(new Fields("str","keyword"),5,
                        new Values("hadoop yarn storm","hadoop框架"),
                        new Values("hadoop mapreduce storm","storm框架"),
                        new Values("hadoop flume flume storm","flume框架"),
                        new Values("hadoop yarn storm","yarn框架"),
                        new Values("kafka yarn kafka","kafka框架"),
                        new Values("spark yarn storm mahout","mahout框架"));

        testSpout.setCycle(false);
        // 构造tridenttopology
        TridentTopology topology = new TridentTopology();
        // 构造DAG  Stream   指定数据采集器
        Stream stream = topology.newStream(SPOUT_ID,testSpout).parallelismHint(1);
        // 指定Tuple中哪个keyvalue对进行 过滤操作
        //stream.shuffle()
        //stream.global()
        //stream.batchGlobal()
        //stream.broadcast()
        //.each(new Fields("str"), new PrintTestFilter())
        // 链式调用
        //过滤处理



        stream.each(new Fields("str"), new HasKeywordFilter()) //对数据的过滤处理
        //.each(new Fields("str"),new PrintTestFilter()) //过滤完后进行打印


        //.each(new Fields("str"), new PrintTestFilter())

        // 对tuple中key名称为str的keyvalue对的value值进行splitfunction操作，
        //产生新的keyvalue对key名称为word
        .each(new Fields("str"), new SplitFunction(),new Fields("word"))
                .each(new Fields("str","keyword","word"),new PrintTestFilter()) //注意这里的tuple追加模式和丢弃模式，如果没有产生新的tuple，则原先的tuple也会丢弃
        // 设置2个executor来执行splitfunction操作
        .parallelismHint(2)
        // tuple---> {"str":"xxxxxxxxx","describe":"xxx","word":"flume"}
        //.each(new Fields("str","describe","word"), new PrintTestFilter())

        // 指定Tuple中只保留 key名称为word的keyvalue对
        .project(new Fields("word"))
//                .each(new Fields("word"),new PrintTestFilter())
        .partitionBy(new Fields("word")) //hash分区
        .groupBy(new Fields("word")).toStream()
//        .persistentAggregate(new MemoryMapState.Factory(),
//                new Count(), new Fields("count")).newValuesStream()
        //.each(new Fields("word"), new  PrintTestFilter())
        //.parallelismHint(4)
        .each(new Fields("word"), new CountFunction(),new Fields("count"))
        .parallelismHint(3)
        .each(new Fields("word","count"), new PrintTestFilter())
        ;

        Config config = new Config();

        if(args == null || args.length <= 0){
            LocalCluster localCluter = new LocalCluster();
            localCluter.submitTopology("wordcountTrident", config, topology.build());
        }else{
            try {
                StormSubmitter.submitTopology(args[0], config, topology.build());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }

    }

}
