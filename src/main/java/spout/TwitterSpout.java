package spout;

import constants.ApplicationConstants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import sun.net.www.protocol.http.AuthenticationHeader;
import twitter4j.*;
import twitter4j.auth.Authorization;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<Status>(100);

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {

            }

            @Override
            public void onStallWarning(StallWarning warning) {

            }

            @Override
            public void onException(Exception ex) {

            }
        };


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(ApplicationConstants.CONSUMER_KEY_KEY);
        cb.setOAuthConsumerSecret(ApplicationConstants.CONSUMER_SECRET_KEY);
        cb.setOAuthAccessToken(ApplicationConstants.ACCESS_TOKEN_KEY);
        cb.setOAuthAccessTokenSecret(ApplicationConstants.ACCESS_TOKEN_SECRET_KEY);
        cb.setUserStreamBaseURL("https://api.twitter.com/labs/1/tweets/stream/sample");

        String token = "AAAAAAAAAAAAAAAAAAAAABzlCQEAAAAAftuxxF1sl5PlZ1evkmzCNgmgt7s%3DLHrfpGHXoyCeD6DJlFTz4an3lWLw8FWgwFkC1FmrFurCuFdak2";

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        twitterStream.user();

    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if(status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }
}
