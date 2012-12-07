/* 	This file is apart of Hadoop-prime

    Hadoop-prime is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    Hadoop-prime is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Hadoop-prime.  If not, see <http://www.gnu.org/licenses/>.
    
    Copyright 2012 Cameron Cook 
 */
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
		query.setCount(100);
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
