package bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


public class TweetOperator extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String title = input.getStringByField("title");
        String publishedAt = input.getStringByField("publishedDate");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
