package com.senspond.storm.hello.bolt;

//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Fields;
//import org.apache.storm.tuple.Tuple;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.senspond.storm.hello.WordCount;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {
    public final static String id = "PrintBolt";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String msg = tuple.getString(0);
            if (msg != null && msg.equals(""))
                System.out.println(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(WordCount.BOLT_FILED_NAME));
    }
}
