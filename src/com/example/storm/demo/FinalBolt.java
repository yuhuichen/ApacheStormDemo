package com.example.storm.demo;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FinalBolt extends BaseRichBolt{
	
	OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("Final Bolt started");
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String message = input.getStringByField("message");
		String uuid = input.getStringByField("uuid");
		Long seqId = input.getLongByField("seqId");
		String sourceComponent = input.getSourceComponent();
		String sourceStreamId = input.getSourceStreamId();
		MessageId  msgId = input.getMessageId();
		Map dataMap = (Map) input.getValueByField("dataMap");
		
		
		String logOutput = String.format("Final Bolt: \n"
				+ "seqId = %d\n"
				+ "uuid = %s\n"
				+ "message = %s\n"
				+ "sourceComponent = %s\n"
				+ "sourceStreamId = %s\n"
				+ "message id = %s\n"
				+ "pet = %s\n",
				seqId, uuid, message, sourceComponent, sourceStreamId, msgId.toString(), dataMap.get("pet")
				);
		
		System.out.println(logOutput);

		
		_collector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("message", "uuid", "seqId", "dataMap"));
		//declarer.declareStream("default", new Fields("message", "uuid", "seqId", "dataMap"));
		//declarer.declareStream("exception", new Fields("message", "uuid", "seqId", "dataMap"));
	}

}
