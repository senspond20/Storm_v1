package com.senspond.storm.hello;


import com.senspond.storm.hello.bolt.PrintBolt;
import com.senspond.storm.hello.bolt.WordCountBolt;
import com.senspond.storm.hello.bolt.WordNormalizerBolt;
import com.senspond.storm.hello.spout.RandomSentenceSpout;
import backtype.storm.Config;

//import org.apache.storm.LocalCluster;
//import org.apache.storm.StormSubmitter;
//import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.tuple.Fields;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HelloTopology  {

    private final static String TOPOLOGY_NAME = "wordcount";

    public static void main(String[] args) {
        Config config = new Config();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RandomSentenceSpout.id, new RandomSentenceSpout(), 2);
        builder.setBolt(WordNormalizerBolt.id, new WordNormalizerBolt(), 2).shuffleGrouping(RandomSentenceSpout.id);
        builder.setBolt(WordCountBolt.id, new WordCountBolt(), 2).fieldsGrouping(WordNormalizerBolt.id, new Fields(WordCount.BOLT_FILED_NAME));
        builder.setBolt(PrintBolt.id, new PrintBolt(), 1).shuffleGrouping(WordCountBolt.id);

//        MyTopology.start(args, config, builder, WordCount.TOPOLOGY_NAME);
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            try {
                LocalCluster cluster =  new LocalCluster();;
                cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
//                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
