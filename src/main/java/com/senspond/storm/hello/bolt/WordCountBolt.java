package com.senspond.storm.hello.bolt;

//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.IRichBolt;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
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

import java.util.*;

public class WordCountBolt implements IRichBolt {
    public final static String id = "WordCountBolt";

    private Map<String, Integer> counters;
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.counters = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);

        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        int num = 8;
        int length = 0;

        counters = sortByValue(counters);

        if (num < counters.keySet().size()) {
            length = num;
        } else {
            length = counters.keySet().size();
        }

        String word = null;

        //增量统计
        int count = 0;
        for (String key : counters.keySet()) {

            if (count >= length) {
                break;
            }

            if (count == 0) {
                word = "[" + key + ":" + counters.get(key) + "]";
            } else {
                word = word + ", [" + key + ":" + counters.get(key) + "]";
            }
            count++;
        }

        word = "The first " + num + ": " + word;
        outputCollector.emit(new Values(word));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static Map<String, Integer> sortByValue(Map<String, Integer> map) {
        if (map == null) {
            return null;
        }
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                Comparable sort1 = (Comparable) ((Map.Entry) o1).getValue();
                Comparable sort2 = (Comparable) ((Map.Entry) o2).getValue();
                return sort2.compareTo(sort1);
           }
        });

        Map result = new LinkedHashMap();

        for (Iterator it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

}
