package com.simon.tridentTopology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import java.util.HashMap;

/**
 * Created by simon on 2016/4/27.
 */
public class TridentTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("random word",new TridentSpout(),2);
        topologyBuilder.setBolt("TOUPPER",new TridentBolt(),2).shuffleGrouping("random word");

        //创建Topology
        StormTopology topology = topologyBuilder.createTopology();

        //配置Config参数
        HashMap conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS,4);
        conf.put(Config.TOPOLOGY_DEBUG,true);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS,0);


        if(args != null && args.length > 0){
            try {
                StormSubmitter.submitTopology(args[0],conf,topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else {
            LocalCluster localCluster = new LocalCluster();
            try {
                StormSubmitter.submitTopology("storm demo",conf,topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
            Utils.sleep(5000);
//            localCluster.shutdown();
        }
    }
}
