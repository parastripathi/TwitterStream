package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class AggregatingBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publishedDate", "title"));
    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        List<Tuple> tuples = tupleWindow.get();
        tuples.sort(Comparator.comparing(this::getTimestamp));

        List<String> list = new ArrayList<>();

        for(Tuple tuple:tuples){
            String text = tuple.getStringByField("title");
            list.add(text);
        }

        String delim = "----***----";
        String combinedList = String.join(delim, list);

        String beginningTimestamp = String.valueOf(getTimestamp(tuples.get(0)));
        String endTimestamp = String.valueOf(getTimestamp(tuples.get(tuples.size() - 1)));

        Values values = new Values(beginningTimestamp+"_"+endTimestamp,combinedList);
        outputCollector.emit(values);
    }

    private Long getTimestamp(Tuple tuple) {
        return tuple.getLongByField("publishedDate");
    }
}