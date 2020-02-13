package bolt;


import entity.DataModified;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;


public class TweetFilter extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        DataModified data = (DataModified) input.getValueByField("data");

        String tweet = data.getText().toLowerCase();

        List<String> listOfKeywords = new ArrayList<>();

        listOfKeywords.add("india");
        listOfKeywords.add("modi");
        listOfKeywords.add("election");


        for(String keyword : listOfKeywords){
            if(tweet.contains(keyword) ){
                collector.emit(new Values(data.getText(), data.getCreatedAt()));
                break;
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare((new Fields("title", "publishedDate")));
    }
}
