package spout;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.oracle.javafx.jmx.json.JSONException;
import entity.Data;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private FileReader fileReader;
    private Boolean completed = false;




    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.spoutOutputCollector = collector;

    }

    @Override
    public void nextTuple() {

        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return;
        }


        String line;
        BufferedReader reader = new BufferedReader(fileReader);
        ObjectMapper mapper = new ObjectMapper();
//        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);


        try {

            while ((line = reader.readLine()) != null) {
                JsonNode tree = mapper.readTree(line);
                JsonNode node = tree.at("/data");
                Data tweetData = mapper.treeToValue(node, Data.class);
                this.spoutOutputCollector.emit(new Values(tweetData));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            completed = true;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
