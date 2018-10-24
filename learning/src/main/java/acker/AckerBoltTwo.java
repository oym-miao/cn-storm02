package acker;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class AckerBoltTwo implements IRichBolt{
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }



    @Override
    public void execute(Tuple input) {

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
