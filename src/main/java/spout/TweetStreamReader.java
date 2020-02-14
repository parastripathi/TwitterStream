package spout;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.databind.JsonNode;
import entity.Data;
import entity.DataModified;

import lombok.Getter;
import lombok.Setter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.storm.utils.Utils;

import config.StormConfig;
import util.WriteThread;

import static constants.ApplicationConstants.SPEC;

@Getter
@Setter
public class TweetStreamReader extends BaseRichSpout {

    private static LinkedBlockingQueue<DataModified> linkedBlockingQueue = new LinkedBlockingQueue<DataModified>();
    private static HttpURLConnection httpURLConnection = null;
    private SpoutOutputCollector spoutOutputCollector;
    private ScheduledExecutorService executorService = null;

    public static HttpURLConnection getHttpURLConnection() {
        return httpURLConnection;
    }

    public static LinkedBlockingQueue<DataModified> getLinkedBlockingQueue() {
        return linkedBlockingQueue;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        URL url;
        StormConfig stormConfig = StormConfig.getInstance();
        try {

            String bearerToken = stormConfig.getProperty("BEARER_TOKEN");
            url = new URL(SPEC);
            httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setRequestProperty("Authorization", bearerToken);

            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(WriteThread::run, 0, 1, TimeUnit.SECONDS);

        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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

    protected void finalize() {
        httpURLConnection.disconnect();
        executorService.shutdown();
    }


}
