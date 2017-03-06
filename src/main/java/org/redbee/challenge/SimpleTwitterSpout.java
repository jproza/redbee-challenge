package org.redbee.challenge;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Preconditions;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.DirectMessage;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings({ "rawtypes", "serial" })
public class SimpleTwitterSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String _username;
	String _pwd;

	public SimpleTwitterSpout(String username, String pwd) {
		Preconditions.checkArgument(!username.equals(""));
		Preconditions.checkArgument(!pwd.equals(""));
		_username = username;
		_pwd = pwd;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}
			@Override public void onDeletionNotice(StatusDeletionNotice sdn) {}
			@Override public void onTrackLimitationNotice(int i) {}
			@Override public void onScrubGeo(long l, long l1) {}
			@Override public void onException(Exception e) {}
			@Override public void onStallWarning(StallWarning warning) {}
		};
		
		UserStreamListener usl = new UserStreamListener() {
			@Override
			public void onException(Exception ex) {}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}
			
			@Override
			public void onStallWarning(StallWarning warning) {}
			
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}
			
			@Override
			public void onUserProfileUpdate(User updatedUser) {
			}
			
			@Override
			public void onUserListUpdate(User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListSubscription(User subscriber, User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListDeletion(User listOwner, UserList list) {
			}
			
			@Override
			public void onUserListCreation(User listOwner, UserList list) {
			}
			
			@Override
			public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
			}
			
			@Override
			public void onUnblock(User source, User unblockedUser) {
			}
			
			@Override
			public void onFriendList(long[] friendIds) {
			}
			
			@Override
			public void onFollow(User source, User followedUser) {
			}
			
			@Override
			public void onFavorite(User source, User target, Status favoritedStatus) {
			}
			
			@Override
			public void onDirectMessage(DirectMessage directMessage) {
			}
			
			@Override
			public void onDeletionNotice(long directMessageId, long userId) {
			}
			
			@Override
			public void onBlock(User source, User blockedUser) {
			}
		};
		
				
		TwitterStreamFactory fact = new TwitterStreamFactory(
				new ConfigurationBuilder().setUser(_username).setPassword(_pwd)
						.build());
		_twitterStream = fact.getInstance();
//		_twitterStream.addListener(listener);
		_twitterStream.addListener(usl);
//		_twitterStream.getUserStream();
//		_twitterStream.getSampleStream();
		_twitterStream.user();
//		_twitterStream.sample();
//		_twitterStream.retweet();
	}

	@Override
	public void nextTuple() {
		Status tw = queue.poll();
		if (tw == null) {
			Utils.sleep(50);
		}
		else {
			if(tw.isRetweet()) {
				Status retweet = tw.getRetweetedStatus();
				HashtagEntity[] hashtagEntities = retweet.getHashtagEntities();
				
				if (hashtagEntities!=null && hashtagEntities.length > 0 ) {
					_collector.emit(new Values(retweet.getUser().getScreenName(), retweet.getText(), retweet.getRetweetCount(), hashtags(hashtagEntities)));
				} else { 
					_collector.emit(new Values(retweet.getUser().getScreenName(), retweet.getText(), retweet.getRetweetCount(), "No HashTag"));
				}
				
			} else {
				HashtagEntity[] hashtagEntities = tw.getHashtagEntities();
				if (hashtagEntities!=null && hashtagEntities.length > 0 ) {
					_collector.emit(new Values(tw.getUser().getScreenName(), tw.getText(), tw.getRetweetCount(), hashtags(hashtagEntities)));				
				} else { 
					_collector.emit(new Values(tw.getUser().getScreenName(), tw.getText(), tw.getRetweetCount(),"No HashTag"));
				}
				
			}
		}
	}

	private static final String hashtags(HashtagEntity[] hashtagEntities) {
		if (hashtagEntities.length == 1) {
			return "#" + ((HashtagEntity)hashtagEntities[0]).getText();
		} else {
			String hash = new String();
			for (int i = 0; i < hashtagEntities.length; i++) {
				if (i == hashtagEntities.length -1) { 
					hash += "#" + ((HashtagEntity)hashtagEntities[i]).getText(); 
				} else { 
					hash += "#" + ((HashtagEntity)hashtagEntities[i]).getText() + " , ";
				}
			}
			return hash;
		}
	}
	
	
	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {}

	@Override
	public void fail(Object id) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("author", "text", "retweetCount", "hashtag"));
	}

}
