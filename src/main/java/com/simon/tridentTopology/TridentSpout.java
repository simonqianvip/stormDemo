package com.simon.tridentTopology;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by simon on 2016/4/27.
 */
public class TridentSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector = null;
    OutputFieldsDeclarer outputFieldsDeclarer = null;
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.outputFieldsDeclarer.declare(new Fields("strName"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;

    }

    public void nextTuple() {
        String[] strings = {"dog","cat","snake","ant","pig","apple","iphone","nokia","dell","HP","IBM"};
        Random random = new Random(strings.length);
        spoutOutputCollector.emit(new Values(strings[random.nextInt()]));
    }
}
