package com.senspond.storm.hello;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class HelloTopology {

    private final static String TOPOLOGY_NAME = "helloworld";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        Config config = new Config();


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
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
