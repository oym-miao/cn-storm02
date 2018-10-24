package acker;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class AckerBolt implements IRichBolt {


    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    int num = 0;
    String url = "";
    String date = null;



    @Override
    public void execute(Tuple input) {
        try {
            date = input.getStringByField("date");
            Double price = Double.parseDouble(input.getStringByField("price"));
            this.collector.ack(input);

            this.collector.emit(input,new Values(""));

        }catch (Exception e){
            this.collector.fail(input);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
