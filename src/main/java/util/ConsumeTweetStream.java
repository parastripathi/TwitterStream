package util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Data;
import entity.DataModified;
import spout.TweetStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumeTweetStream {

    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    private static final String JSON_ROOT = "/data";

    public static void run() {

        HttpURLConnection httpURLConnection = TweetStreamReader.getHttpURLConnection();
        LinkedBlockingQueue<DataModified> linkedBlockingQueue = TweetStreamReader.getLinkedBlockingQueue();

        InputStream in = null;
        try {
            in = httpURLConnection.getInputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        ObjectMapper mapper = new ObjectMapper();
        int i = 0;


        try {
            while ((line = reader.readLine()) != null) {

                JsonNode tree = mapper.readTree(line);
                JsonNode node = tree.at(JSON_ROOT);
                Data data = mapper.treeToValue(node, Data.class);

                DataModified dataModified = new DataModified();

                dataModified.setAuthorId(data.getAuthorId());
                dataModified.setId(data.getId());
                dataModified.setText(data.getText());

                String myDate = data.getCreatedAt();
                SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
                Date date = sdf.parse(myDate);
                long millis = date.getTime();

                dataModified.setCreatedAt(millis);

                i++;
                linkedBlockingQueue.add(dataModified);
                if (i > 1000)
                    break;
            }
        } catch (ParseException | IOException e) {
            System.out.println(e.getMessage());
        }

    }
}
