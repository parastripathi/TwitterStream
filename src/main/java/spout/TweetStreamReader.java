package spout;

import com.fasterxml.jackson.databind.JsonNode;
import entity.Data;
import entity.DataModified;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.utils.Utils;

public class TweetStreamReader extends BaseRichSpout {

    private static LinkedBlockingQueue<DataModified> linkedBlockingQueue = new LinkedBlockingQueue<DataModified>();
    private static HttpURLConnection httpURLConnection = null;
    private SpoutOutputCollector spoutOutputCollector;
    private ScheduledExecutorService executorService = null;

    public static void run() {
        try {
            InputStream in = httpURLConnection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            ObjectMapper mapper = new ObjectMapper();
            int i = 0;

            int qsize = linkedBlockingQueue.size();

            while ((line = reader.readLine()) != null) {
                JsonNode tree = mapper.readTree(line);
                JsonNode node = tree.at("/data");
                Data data = mapper.treeToValue(node, Data.class);

                DataModified dataModified = new DataModified();

                dataModified.setAuthorId(data.getAuthorId());
                dataModified.setId(data.getId());
                dataModified.setText(data.getText());

                String myDate = data.getCreatedAt();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                Date date = sdf.parse(myDate);
                long millis = date.getTime();

                dataModified.setCreatedAt(millis);

                i++;
                linkedBlockingQueue.add(dataModified);
                if (i > 1000)
                    break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
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

            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(TweetStreamReader::run, 0, 1, TimeUnit.SECONDS);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        this.spoutOutputCollector = collector;


    }

    @Override
    public void nextTuple() {

        DataModified data = linkedBlockingQueue.poll();
        if (data == null) {
            Utils.sleep(50);
        } else {
            spoutOutputCollector.emit(new Values(data));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }

    protected void finalize(){
        httpURLConnection.disconnect();
        executorService.shutdown();
    }


}
