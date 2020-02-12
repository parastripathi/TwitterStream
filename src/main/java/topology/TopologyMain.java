package topology;

import bolt.TweetOperator;
import bolt.TweetFilter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.TweetStreamReader;

import java.io.File;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        String path = new File("src/main/resources/word.txt").getAbsolutePath();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweet-reader",new TweetStreamReader());

        builder.setBolt("tweet-filter",new TweetFilter()).shuffleGrouping("tweet-reader");
        builder.setBolt("tweet-operator",new TweetOperator()).shuffleGrouping("tweet-filter");

        Config config = new Config();
        config.setDebug(true);


        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("my-first-topology",config,builder.createTopology());
        Thread.sleep(10000);

        localCluster.shutdown();

    }


}
