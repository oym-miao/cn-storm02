package acker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
/**
 *
 * 功能描述: acker机制，如何控制重发次数？
 *
 * @param:
 * @return:
 * @auther: wuyue
 * @date: 2018/8/9 下午10:57
 */
public class AckerSpout implements IRichSpout {


    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;
    String str = null;


    //线程安全的map.存储emit过的tuple
    private ConcurrentHashMap<Object,Values> _pendingTuples = new ConcurrentHashMap<>();
    //存储失败的的tuple和其失败的次数
    private ConcurrentHashMap<Object,Integer> fail_pending = new ConcurrentHashMap<>();

    //修改下 内存 持久化
    //数据库里面

    SpoutOutputCollector collector;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.fis = new FileInputStream("order_track.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        this.br = new BufferedReader(isr);
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        try{
            while((str = this.br.readLine())!=null) {
                UUID msgid = UUID.randomUUID();
                //获取数据
                String data[] = str.split("\t");
                String date = data[2].substring(0,10);
                Values values = new Values(date, data[1]);
                this._pendingTuples.put(msgid, values); //内存缓存
                this.collector.emit(values, msgid); //注意这里的msgid,不能重复
                System.out.println("nextTuple()===>"+_pendingTuples.size());

            }
        }catch (Exception e){

        }finally {

        }

    }

    @Override
    public void ack(Object msgId) {
        this._pendingTuples.remove(msgId);
        System.out.println("ack======>"+_pendingTuples.size());
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("fail======>"+fail_pending.size());
        Integer fail_count = fail_pending.get(msgId);
        System.out.println("fail_count======>"+fail_count);
        if(fail_count == null){
            fail_count = 0;
        }
        fail_count++;
        if(fail_count>3){
            fail_pending.remove(msgId); //不发送了
        }else {
            fail_pending.put(msgId,fail_count);
            this.collector.emit(this._pendingTuples.get(msgId),msgId); //重传
            System.out.println("===========================>发射");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date","price"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
