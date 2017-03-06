package org.redbee.challenge;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class Dashboard extends BaseRichBolt {
	
	private OutputCollector collector;
	private static final Logger logger = LoggerFactory.getLogger(Dashboard.class);
//	long retweetThreshold;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger.info("Prepared Dashboard");
//		retweetThreshold = 1L;
	}

	@Override
	public void execute(Tuple input) {
//		Status tweet = (Status) input.getValueByField("tweet");
		String author = input.getStringByField("author");
		String text = input.getStringByField("text");
		Long retweetCount = input.getLongByField("retweetCount");
		String hashtag = input.getStringByField("hashtag");
//		String[] tags = (String[]) input.getValueByField("hashtagEntities");
		
//		if(retweetCount > retweetThreshold) {
//			retweetThreshold = retweetCount;
			
		String[] tags = readHastagFilters();
		
			
			if (checkifExistsTag(hashtag , tags)) { 
				logger.info("@" + author + " got " + String.valueOf(retweetCount) + " retweets for : " + text + " hashtag : " + hashtag);
			} else { 
				logger.info("@" + author + " got " + String.valueOf(retweetCount) + " retweets for : " + text + " ***Do not show on hashtags*** ");
			}
			
			
//		}
		
		
		collector.ack(input);
	}
	
	
	private static final String[] readHastagFilters() { 
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader("resources/hashtags.properties"));
		String str;
		List<String> list = new ArrayList<String>();
		while((str = in.readLine()) != null){
		    list.add("#" + str);
		}
		String[] stringArr = list.toArray(new String[0]);
		
		return stringArr;
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
//	@SuppressWarnings("resource")
//	public static void main(String[] args) {
//		BufferedReader in;
//			try {
//				in = new BufferedReader(new FileReader("resources/hashtags.properties"));
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//	}
	
	
	
	private static final boolean checkifExistsTag(String tag, String[] fileTags) {
		List<String> items = Arrays.asList(tag.split(","));
		for (Iterator iterator = items.iterator(); iterator.hasNext();) {
			String t = (String) iterator.next();
			if (Arrays.asList(fileTags).contains(t)) {
				return true;
			}
		}
		return false;
	}
	
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
