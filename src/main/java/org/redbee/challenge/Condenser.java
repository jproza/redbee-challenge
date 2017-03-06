package org.redbee.challenge;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Condenser {


	static final String CONDENSER_NAME = "redbee";
	static final String TWITTER_USERNAME = "";
	static final String TWITTER_PASSWORD = "";
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("twitterStream", new SimpleTwitterSpout(TWITTER_USERNAME, TWITTER_PASSWORD));
		b.setBolt("Dashboard", new Dashboard()).shuffleGrouping("twitterStream");
		
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(CONDENSER_NAME, config, b.createTopology());
		
		Runtime.getRuntime().addShutdownHook(new Thread()	{
			@Override
			public void run()	{
				cluster.killTopology(CONDENSER_NAME);
				cluster.shutdown();
			}
		});
		

	}

}


