package spout;

import com.fasterxml.jackson.databind.JsonNode;
import entity.Data;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.utils.Utils;

public class WordReader extends BaseRichSpout {

    private static LinkedBlockingQueue<Data> linkedBlockingQueue = new LinkedBlockingQueue<Data>();
    private SpoutOutputCollector spoutOutputCollector;
    private static HttpURLConnection httpURLConnection = null;


    public static void run() {
        try {
            InputStream in = httpURLConnection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            ObjectMapper mapper = new ObjectMapper();
            int i = 0;
            while ((line = reader.readLine()) != null) {
                JsonNode tree = mapper.readTree(line);
                JsonNode node = tree.at("/data");
                Data data = mapper.treeToValue(node, Data.class);
                i++;
                linkedBlockingQueue.add(data);
                if (i > 1000)
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        URL url;

        try {

            url = new URL("https://api.twitter.com/labs/1/tweets/stream/sample");
            httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setRequestProperty("Authorization",
                    "Bearer AAAAAAAAAAAAAAAAAAAAABzlCQEAAAAAftuxxF1sl5PlZ1evkmzCNgmgt7s"
                            + "%3DLHrfpGHXoyCeD6DJlFTz4an3lWLw8FWgwFkC1FmrFurCuFdak2");

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(WordReader::run, 0, 1, TimeUnit.SECONDS);

        } catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            httpURLConnection.disconnect();
        }
        this.spoutOutputCollector = collector;

    }

    @Override
    public void nextTuple() {

        Data data = linkedBlockingQueue.poll();
        if(data == null) {
            Utils.sleep(50);
        } else {
            spoutOutputCollector.emit(new Values(data));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
