package bolt;


import entity.DataModified;
import entity.RedisStorage;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.util.*;


public class TweetFilter extends BaseBasicBolt {
    int countFound = 0;
    int totalCount = 0;
    Vector<Vector<String>> tupleBucket = new Vector<Vector<String>>();;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 300);
        return conf;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        if (isTickTuple(input)) {
            tupleBucket.clear();
            return;
        }


        DataModified data = (DataModified) input.getValueByField("data");

        String tweet = data.getText().toLowerCase();
        totalCount++;
        List<String> listOfKeywords = new ArrayList<>();

        listOfKeywords.add("india");
        listOfKeywords.add("modi");
        listOfKeywords.add("election");
        //listOfKeywords.add(" ");


        for(String keyword : listOfKeywords){
           /* if(tweet.contains(keyword) ){
                countFound++;
                collector.emit(new Values(data.getText(), data.getCreatedAt()));
                break;
            }*/
           if(tweet.contains(keyword)){
               Vector<String> v = new Vector<>();
               v.clear();
               v.add(data.getText());
               v.add(data.getCreatedAt().toString());
               tupleBucket.add(v);

               RedisStorage redisStorage = new RedisStorage();
               redisStorage.setTupleBucket(tupleBucket);
               collector.emit(new Values(redisStorage));
               //collector.emit(new Values(v));

             //  p.zadd("key", timestamp, tweet)
           }


        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare((new Fields("tupleBucket")));
    }
}
