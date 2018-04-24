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

public class FirstBolt extends BaseRichBolt{
	
	OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
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
		
		
		String logOutput = String.format("First Bolt: \n"
				+ "seqId = %d\n"
				+ "uuid = %s\n"
				+ "message = %s\n"
				+ "sourceComponent = %s\n"
				+ "sourceStreamId = %s\n"
				+ "message id = %s\n",
				seqId, uuid, message, sourceComponent, sourceStreamId, msgId.toString()
				);
		
		System.out.println(logOutput);
		
		Map dataMap = new LinkedHashMap<String, String> ();
		
		
		Values values =  new Values(message, uuid, seqId, dataMap);
		if (seqId%2 == 1 ) {
			
			_collector.emit("defaultStream", values);
		}else {
			_collector.emit("exceptionStream", values);
		}
		
		_collector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("message", "uuid", "seqId", "dataMap"));
		declarer.declareStream("defaultStream", new Fields("message", "uuid", "seqId", "dataMap"));
		declarer.declareStream("exceptionStream", new Fields("message", "uuid", "seqId", "dataMap"));
	}

}
