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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.mcavallo.opencloud.Cloud;
import org.mcavallo.opencloud.Tag;

import edu.american.student.conf.Constants;
import edu.american.student.conf.TwitterCloudConfiguration;
import edu.american.student.conf.TwitterWordCountConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.foreman.ProcessForeman;
import edu.american.student.process.TwitterTagCloudProcess;
import edu.american.student.process.TwitterWordCountProcess;
import edu.american.student.util.CloudGenerator;

/**
 * Difficulty: 5 - Expert
 * Full Explanation: FIXME
 * Relevant Files: example-resources/example-resources/gen/
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: We will ingest from twitter.
 * TwitterWordCountProcess starts, it will count each instance of a word per tweet, then collect those words together in Accumulo
 * TwitterTagCloudProcess starts, it will take entries of each word count and make a Tag cloud with it
 * 
 * 
 * @author cam
 *
 */
public class TwitterTagCloudProcessing
{
	private static AccumuloForeman aForeman = new AccumuloForeman();
	private static Cloud cloud = new Cloud();
	
	public static void main(String[] args) throws ProcessException, IOException
	{
		aForeman.connect();
		//Configure a Open Cloud Instance
		cloud.setMaxWeight(38.0);//Max Font Size
		cloud.setMinWeight(10.0);//Min Font Size
		cloud.setThreshold(0.0);//Score minimum to display
		cloud.setDefaultLink("http://www.google.com/");// They show up as links. They all link to google
		ProcessForeman.ingestTwitter();//ingest all our twitter data from spidering
		
		/*
		 * This process counts the number of instances of each word and saves them as TAGs
		 */
		TwitterWordCountConfiguration conf = new TwitterWordCountConfiguration();
		conf.setTwitterWordCountMapper(TwitterWordCountMapper.class);
		conf.setTwitterWordCountReducer(TwitterWordCountReducer.class);
		
		TwitterWordCountProcess wordCountProcess = new TwitterWordCountProcess();
		wordCountProcess.initalize(conf);
		wordCountProcess.start();
		
		/*
		 * This process goes through the TAGs. Then adds tags to the open cloud instance
		 */
		TwitterCloudConfiguration tagCloudConf = new TwitterCloudConfiguration();
		tagCloudConf.setTwitterCloudMapper(TwitterTagCloudMapper.class);
		
		TwitterTagCloudProcess tagCloudProcess = new TwitterTagCloudProcess();
		tagCloudProcess.initalize(tagCloudConf);
		tagCloudProcess.start();
		
		//Generates the cloud using open cloud. Pass it where you want it to save
		CloudGenerator.generateCloud(cloud,"example-resources/gen");
	}
	/**
	 * Each Mapper receives a LINE column family entry.
	 * Splits up the line into words, organizes them by number of instance and sends them to the Reducer
	 * @author cam
	 *
	 */
	public static class TwitterWordCountMapper extends Mapper<Key,Value,Text,IntWritable>	
	{
		@Override
		public void map(Key key,Value value,Context context)
		{
			String line = value.toString();

			String[] words = line.split(" ");
			// This is a dictionary, it works like this [myWord: instancesOfMyWord]
			Map<String, Integer> dictionary = new HashMap<String, Integer>();
			for (String word : words)
			{
				if (dictionary.containsKey(word))
				{
					int previousInstances = dictionary.get(word);
					dictionary.remove(word);
					dictionary.put(word, previousInstances++);
				}
				else
				{
					dictionary.put(word, 1);
				}
			}

			// iterate over the dictionary entries
			Iterator<Entry<String, Integer>> dictionaryIterator = dictionary.entrySet().iterator();
			while (dictionaryIterator.hasNext())
			{
				Entry<String, Integer> entry = dictionaryIterator.next();
				String word = entry.getKey();
				int numberOfInstances = entry.getValue();
				try
				{
					Text k = new Text(word);
					IntWritable v = new IntWritable(numberOfInstances);
					context.write(k, v);
				}
				catch (Exception e)
				{
					String gripe = "Couldn't write to context!";
					throw new StopMapperException(gripe, e);
				}
			}
		}
	}

	/**
	 * Each Reducer counts the total instances of a word (tag) for a file. then adds them to Accumulo
	 * 
	 * ROW: TAG
	 * COLUMN FAMILY: TAG
	 * COLUMN QUALIFER: <word>
	 * VALUE: <instance count>
	 * @author cam
	 *
	 */
	public static class TwitterWordCountReducer extends Reducer<Text,IntWritable,NullWritable,NullWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		{
			String word = key.toString();
			int total = 0;
			Iterator<IntWritable> valuesIterator = values.iterator();
			while (valuesIterator.hasNext())
			{
				total += valuesIterator.next().get();
			}
			System.out.println("Word = " + word + " total use = " + total);
			String table = Constants.getDefaultTable();
			String row = "TAG";
			String fam = "TAG";
			String qual = word;
			String value = total+"";
			try
			{
				aForeman.add(table, row, fam, qual, value);
			}
			catch (RepositoryException e)
			{
				String gripe = "Failed to write to Accumulo";
				throw new StopMapperException(gripe,e);
			}
			
		}
	}

	/**
	 * Each Mapper's responsibility is to take a tag total and feed it to the TagCloudGenerator
	 * 
	 * @author cam
	 *
	 */
	public static class TwitterTagCloudMapper extends Mapper<Key,Value,NullWritable,NullWritable>
	{
		@Override
		public void map(Key key,Value value,Context context)
		{
			//word
			String qual = key.getColumnQualifier().toString();
			//number of words
			String val = value.toString();
			cloud.addTag(new Tag(qual,Integer.parseInt(val)));
		}
	}
}
