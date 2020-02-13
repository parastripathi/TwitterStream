package bolt;

import entity.RedisStorage;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.Vector;

public class RedisTransactionBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        org.apache.storm.redis.common.config.JedisPoolConfig poolConfig = new org.apache.storm.redis.common.config.JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379).build();
        JedisPool pool = new JedisPool(new JedisPoolConfig(), poolConfig.getHost(), poolConfig.getPort());
        Jedis jedis = pool.getResource();
        Pipeline p = jedis.pipelined();
        RedisStorage redisStorage = new RedisStorage();
        redisStorage = (RedisStorage) input.getValueByField("tupleBucket");
        Vector<Vector<String>> tupleBucket = redisStorage.getTupleBucket();
       // Vector<Vector<String>> tupleBucket = (Vector<Vector<String>>) input.getValueByField("tupleBucket");
        for (Vector<String> val : tupleBucket ) {
            String tweet = val.get(0);
            String time = val.get(1);
            p.zadd("todaysDate",Double.parseDouble(time),tweet);

        }

        p.sync();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
