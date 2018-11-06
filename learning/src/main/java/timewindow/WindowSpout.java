package timewindow;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;


/***
 * 1. 读文件 2. 获取相应的字段发射 bolt
 */
public class WindowSpout implements IRichSpout{

	private static final long serialVersionUID = 1L;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;			
	SpoutOutputCollector collector = null;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			this.fis = new FileInputStream("order_track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	String str = null;
	@Override
	public void nextTuple() {

		//如果数据量大，写一个同步机制 这里还需要开一个线程池

		try {
			while ((str = this.br.readLine()) != null) {
				//过滤操作   数据例子: 1	4	2014-01-07 09:40:49
				String data[] = str.split("\t");
				String date = data[2].substring(0, 10);  //取出它的时间
				System.out.println("==========================================");
				collector.emit(new Values(date,data[1]));   //发射的啥？ object    [2014-01-07, 4]

//				Thread.sleep(3000);
				//to do
			}
		} catch (Exception e) {

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","price"));
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
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

		
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	

}
