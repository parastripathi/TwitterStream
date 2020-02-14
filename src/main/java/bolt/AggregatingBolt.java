package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import config.JedisConfig;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class AggregatingBolt extends BaseWindowedBolt {

    private static final String YYYY_MM_DD = "yyyy/MM/dd";
    private static final String TEXT = "text";
    private static final String CREATED_AT = "createdAt";

    private static Jedis jedis = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        jedis = JedisConfig.getJedisInstance();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        Transaction transaction = jedis.multi();

        DateFormat dateFormat = new SimpleDateFormat(YYYY_MM_DD);
        Date date = new Date();
        String myKey = dateFormat.format(date);
        List<Tuple> tuples = tupleWindow.get();
        tuples.sort(Comparator.comparing(this::getTimestamp));

        for (Tuple tuple : tuples) {
            String text = tuple.getStringByField(TEXT);
            Long time = tuple.getLongByField(CREATED_AT);
            transaction.zadd(myKey, Double.valueOf(String.valueOf(time)), text);

        }

        transaction.exec();

    }

    private Long getTimestamp(Tuple tuple) {
        return tuple.getLongByField(CREATED_AT);
    }
}