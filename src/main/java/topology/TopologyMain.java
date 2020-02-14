package topology;

import bolt.AggregatingBolt;
import bolt.TweetFilter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import spout.TweetStreamReader;

import static constants.ApplicationConstants.*;

public class TopologyMain {


    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TWEET_READER, new TweetStreamReader());
        builder.setBolt(TWEET_FILTER, new TweetFilter()).shuffleGrouping(TWEET_READER);

        BaseWindowedBolt aggregating = new AggregatingBolt()
                .withTimestampField(PUBLISHED_DATE)
                .withLag(BaseWindowedBolt.Duration.seconds(1))
                .withWindow(BaseWindowedBolt.Duration.seconds(10));


        builder.setBolt(AGGREGATING_BOLT, aggregating).shuffleGrouping(TWEET_FILTER);
        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TWITTER_TOPOLOGY, config, builder.createTopology());

    }


}
