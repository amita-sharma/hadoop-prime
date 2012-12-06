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
package edu.american.student.examples.level5;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import edu.american.student.conf.Constants;
import edu.american.student.conf.TwitterIngestConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.process.TwitterIngestProcess;
import edu.american.student.util.AccumuloAdministrator;
import edu.american.student.util.TwitterSpider;


/**
 * Difficulty: 5 - Expert
 * Full Explanation: FIXME
 * Relevant Files: example-resources/twitterHandlesToIngest
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: Using our Twitter spider, grab the last 10 tweets from each 
 * twitter user (line-delimited). Insert these tweets into Accumulo
 * 
 * @author cam
 *
 */
public class TwitterIngest
{
	private static AccumuloForeman aForeman = new AccumuloForeman();

	public static void main(String[] args) throws ProcessException
	{
		AccumuloAdministrator.setup();
		aForeman.connect();
		
		TwitterIngestConfiguration conf = new TwitterIngestConfiguration();
		conf.setTwitterIngestDefinitions("example-resources/twitterHandlesToIngest.txt");
		conf.setTwitterIngestMapper(TwitterIngestMapper.class);

		TwitterIngestProcess process = new TwitterIngestProcess();
		process.initalize(conf);
		process.start();
	}

	/**
	 * Each Mappers responsibility is to grab a line from twitterHandlesToIngest.txt
	 * That line represents a twitter handle
	 * Twitter spider then grabs the last 10 statuses from that twitter user
	 * Ingests that tweet
	 * 
	 * ROW: <twitterHandle><UUID>
	 * COLUMN FAMILY: "LINE" // this is to stay consistent with our previous Ingest
	 * COLUMN QUALIFIER: <twitterHandle>
	 * VALUE: Tweet's text
	 * @author cam
	 *
	 */
	public static class TwitterIngestMapper extends Mapper<LongWritable, Text, Key, Value>
	{
		public void map(LongWritable ik, Text iv, Context context)
		{
			String twitterHandleToIngest = iv.toString();
			try
			{
				List<Status> tweets = TwitterSpider.spider(twitterHandleToIngest);
				for (Status tweet : tweets)
				{
					System.out.println(tweet.getText());
					String table = Constants.getDefaultTable();
					String row = twitterHandleToIngest+UUID.randomUUID().toString();
					String fam = "LINE";
					String qual = twitterHandleToIngest;
					String value = tweet.getText();
					aForeman.add(table, row, fam, qual, value);

				}
			}
			catch (Exception e)
			{
				String gripe = "Twitter ingest failed!";
				throw new StopMapperException(gripe, e);
			}
		}
	}
}
