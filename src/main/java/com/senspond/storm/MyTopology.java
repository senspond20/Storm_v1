package com.senspond.storm;

//import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
//import org.apache.storm.StormSubmitter;
//import org.apache.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class MyTopology {

    public static void start(String[] args, Config config,  TopologyBuilder builder, String topologyName){
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster =  new LocalCluster();;
            cluster.submitTopology(topologyName, config, builder.createTopology());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                cluster.killTopology(topologyName);
                cluster.shutdown();
            }
        }
    }
}
