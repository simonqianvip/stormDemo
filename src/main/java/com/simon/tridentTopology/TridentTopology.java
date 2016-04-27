package com.simon.tridentTopology;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by simon on 2016/4/27.
 */
public class TridentTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("random word",new TridentSpout(),2);
        topologyBuilder.setBolt("TOUPPER",new TridentBolt(),2).shuffleGrouping("random word");

        StormTopology topology = topologyBuilder.createTopology();

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);
        conf.setNumAckers(0);


        try {
            StormSubmitter.submitTopology("storm demo",conf,topology);
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
