package computer;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FileBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	Integer num = 0;
	@Override
	public void execute(Tuple input) {
		String log = input.getStringByField("log");
		System.out.println("bolt-print == "+log);
		num ++ ;
		System.out.println("num == "+num);
		this.collector.emit(new Values(num));
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("num"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
