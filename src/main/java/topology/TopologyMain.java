package topology;

import bolt.WordCounter;
import bolt.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spout.WordReader;

import java.io.File;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        String path = new File("src/main/resources/word.txt").getAbsolutePath();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-reader",new WordReader());

        builder.setBolt("word-normalizer",new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter",new WordCounter()).shuffleGrouping("word-normalizer");

        Config config = new Config();
        config.put("wordsFile",path);
        config.setDebug(true);



        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("my-first-topology",config,builder.createTopology());
        Thread.sleep(10000);

        localCluster.shutdown();

    }


}
