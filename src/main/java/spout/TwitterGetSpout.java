package spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class TwitterGetSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {

        try {

            URL urlForGetRequest = new URL("https://api.twitter.com/labs/1/tweets?ids=1067094924124872705&format=detailed");
            String readLine = null;
            HttpURLConnection connection = (HttpURLConnection) urlForGetRequest.openConnection();
            connection.setRequestMethod("GET");
            String jwtAuth = "Bearer " + "AAAAAAAAAAAAAAAAAAAAABzlCQEAAAAAftuxxF1sl5PlZ1evkmzCNgmgt7s%3DLHrfpGHXoyCeD6DJlFTz4an3lWLw8FWgwFkC1FmrFurCuFdak2";
            connection.setRequestProperty ("Authorization", jwtAuth);


            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream()));
                StringBuffer response = new StringBuffer();
                while ((readLine = in.readLine()) != null) {
                    response.append(readLine);
                }
                in.close();

                System.out.println("JSON String Result " + response.toString());

            } else {
                System.out.println("GET NOT WORKED");
            }

        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
