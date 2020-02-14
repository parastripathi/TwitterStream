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


import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class TweetFilter extends BaseBasicBolt {

    private static StormConfig stormConfig = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        DataModified data = (DataModified) input.getValueByField("data");
        String tweet = data.getText().toLowerCase();

        stormConfig = StormConfig.getInstance();
        String filterList = stormConfig.getProperty("FILTER_LIST");
        List<String> listOfKeywords = Arrays.asList(filterList.split(","));

        for (String keyword : listOfKeywords) {
            if (tweet.contains(keyword)) {
                collector.emit(new Values(data.getText(), data.getCreatedAt()));
                break;
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare((new Fields("text", "createdAt")));
    }
}
