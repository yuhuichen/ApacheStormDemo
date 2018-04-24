package com.example.storm.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class DemoTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new DemoSpout(), 1);
		builder.setBolt("firstBolt", new FirstBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("secondBolt", new SecondBolt(), 1).shuffleGrouping("firstBolt", "defaultStream");
		builder.setBolt("exceptionBolt", new ExceptionBolt(), 1).shuffleGrouping("firstBolt", "exceptionStream");
		builder.setBolt("finalBolt", new FinalBolt(), 1)
			.shuffleGrouping("secondBolt", "defaultStream")
			.shuffleGrouping("exceptionBolt", "exceptionStream");
		
		Config conf = new Config();
		//conf.setNumWorkers(1);
		conf.setMaxTaskParallelism(3);
		
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("Demo", conf, builder.createTopology());
		
		Utils.sleep(60000);
		
		cluster.killTopology("Demo");
		cluster.shutdown();

	}

}
