package com.simon.tridentTopology;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by simon on 2016/4/27.
 */
public class TridentBolt extends BaseRichBolt {
    OutputCollector outputCollector = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String random_str = tuple.getString(0);
        String upperCase = random_str.toUpperCase();

        try {
            FileWriter fileWriter = new FileWriter("D:\\"+ UUID.randomUUID()+".txt");
            fileWriter.write(upperCase);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
