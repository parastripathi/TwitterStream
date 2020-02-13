package topology;

import bolt.AggregatingBolt;
import bolt.TweetOperator;
import bolt.TweetFilter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.ITuple;
import spout.TweetStreamReader;

import java.io.File;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        String host = "127.0.0.1";
        int port = 6379;

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();

        RedisStoreMapper storeMapper = setupStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweet-reader", new TweetStreamReader());

        builder.setBolt("tweet-filter", new TweetFilter()).shuffleGrouping("tweet-reader");

//        builder.setBolt("store-bolt",storeBolt,1).shuffleGrouping("tweet-filter");


        BaseWindowedBolt aggregating = new AggregatingBolt()
                .withTimestampField("publishedDate")
                .withLag(BaseWindowedBolt.Duration.seconds(1))
                .withWindow(BaseWindowedBolt.Duration.seconds(10));


        builder.setBolt("aggregating-bolt", aggregating).shuffleGrouping("tweet-filter");
        builder.setBolt("store-bolt", storeBolt, 1).shuffleGrouping("aggregating-bolt");


        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("my-first-topology", config, builder.createTopology());
        Thread.sleep(10000);


    }


    private static RedisStoreMapper setupStoreMapper() {
        return new TweetCountStoreMapper();
    }

    private static class TweetCountStoreMapper implements RedisStoreMapper {
        private RedisDataTypeDescription description;

        TweetCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.STRING);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("publishedDate");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return tuple.getStringByField("title");
        }
    }


}
