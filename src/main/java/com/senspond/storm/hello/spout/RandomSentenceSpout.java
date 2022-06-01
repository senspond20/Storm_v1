package com.senspond.storm.hello.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.senspond.storm.hello.WordCount;


//import org.apache.storm.spout.SpoutOutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichSpout;
//import org.apache.storm.tuple.Values;
//import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    public final static String id = "RandomSentence";

    private SpoutOutputCollector spoutOutputCollector;
    private Random random;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(2000);
        String[] sentences = new String[]{
                "C/C++ C# JAVA Python Scala Groovy",
                "MFC .NET Spring",
                "NodeJs React Vue Svelte",
                "Hadoop Storm Spark"};
        String sentence = sentences[random.nextInt(sentences.length)];
        spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(WordCount.BOLT_FILED_NAME));
    }
}
