package com.example.storm.demo;

import java.util.Map;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class DemoSpout extends BaseRichSpout{
	
	SpoutOutputCollector _collector;
	Long msgCounter = 0L;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		System.out.println("Demo Spout started");
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(2000);
		
		String uuid = UUID.randomUUID().toString();
		String message = "msg-" + uuid;
		Long seqId =  ++msgCounter;
		
		Values values = new Values(message, uuid, seqId);
		System.out.println("**************");
		
		if (seqId <=5) {
			_collector.emit(values, uuid);
		}
		//_collector.emit(values);
	}
	
	@Override
	public void ack(Object msgId) {
		System.out.println("Ack'd: " + msgId.toString());
		System.out.println("=============");
	}
	
	@Override
	public void fail(Object msgId) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("message", "uuid", "seqId"));
	}
	
	

}
