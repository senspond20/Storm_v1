package com.senspond.storm.hello.bolt;

//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.IRichBolt;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.senspond.storm.hello.WordCount;

import java.util.Map;

public class WordNormalizerBolt implements IRichBolt {
    private OutputCollector outputCollector;
    public final static String id = "WordNormalizerBolt";


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");

        for (String word : words) {
            this.outputCollector.emit(new Values(word));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(WordCount.BOLT_FILED_NAME));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
