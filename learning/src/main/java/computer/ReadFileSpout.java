package computer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


/***
 * 1. 读文件 2. 获取相应的字段发射 bolt
 */
public class ReadFileSpout implements IRichSpout{


	private static final long serialVersionUID = 1L;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;			

	SpoutOutputCollector collector = null;
	
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			this.fis = new FileInputStream("session_track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
    String str = null;
	@Override
	public void nextTuple() {
		try {
			while ((str = this.br.readLine()) != null) {

				collector.emit(new Values(str));   //发射的啥？ object
				
//				Thread.sleep(3000);
				//to do
			}
		} catch (Exception e) {

		}
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {
		//这里是不是要写业务
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	

}
