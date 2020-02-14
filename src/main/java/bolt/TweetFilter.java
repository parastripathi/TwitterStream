package bolt;

import entity.DataModified;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import config.StormConfig;


import java.util.Map;


public class TweetFilter extends BaseBasicBolt {

    public static final String TWEET = "tweet";
    public static final String TEXT = "text";
    public static final String CREATED_AT = "createdAt";
    public static final String PROPERTY_NAME = "FILTER_LIST";

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        DataModified data = (DataModified) input.getValueByField(TWEET);
        String tweet = data.getText().toLowerCase();

        StormConfig stormConfig = StormConfig.getInstance();
        String filterList = stormConfig.getProperty(PROPERTY_NAME);
        String[] listOfKeywords = filterList.split(",");

        for (String keyword : listOfKeywords) {
            if (tweet.contains(keyword)) {
                collector.emit(new Values(data.getText(), data.getCreatedAt()));
                break;
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare((new Fields(TEXT, CREATED_AT)));
    }
}
