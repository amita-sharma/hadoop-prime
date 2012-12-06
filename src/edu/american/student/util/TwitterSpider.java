package edu.american.student.util;

import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import edu.american.student.conf.Constants;
import edu.american.student.exception.SpiderException;

public class TwitterSpider
{
	public static List<Status> spider(String handle) throws SpiderException
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey(Constants.getTwitterOAuthConsumerKey())
			.setOAuthConsumerSecret(Constants.getTwitterOAuthConsumerSecret())
			.setOAuthAccessToken(Constants.getTwitterOAuthAccessToken())
			.setOAuthAccessTokenSecret(Constants.getTwitterOAuthAccessTokenSecret());
		TwitterFactory tf = new TwitterFactory(cb.build());
		
		Twitter twitter = tf.getInstance();
		Query query = new Query("from:"+handle.replace("@", ""));
		QueryResult result;
		try
		{
			result = twitter.search(query);
			List<Status> toReturn = result.getTweets();
			return toReturn;
		}
		catch (TwitterException e)
		{
			String gripe = "Twitter spider failed to...err spider!";
			throw new SpiderException(gripe, e);
		}
	}
}
