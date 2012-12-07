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

public class TwitterTagCloudProcessing
{
	private static AccumuloForeman aForeman = new AccumuloForeman();
	private static Cloud cloud = new Cloud();
	
	public static void main(String[] args) throws ProcessException, IOException
	{
		aForeman.connect();
		cloud.setMaxWeight(38.0);
		cloud.setMinWeight(10.0);
		cloud.setThreshold(0.0);
		cloud.tags(new Tag.ScoreComparatorDesc());
		cloud.setDefaultLink("http://www.google.com/");
		ProcessForeman.ingestTwitter();
		
		TwitterWordCountConfiguration conf = new TwitterWordCountConfiguration();
		conf.setTwitterWordCountMapper(TwitterWordCountMapper.class);
		conf.setTwitterWordCountReducer(TwitterWordCountReducer.class);
		
		TwitterWordCountProcess wordCountProcess = new TwitterWordCountProcess();
		wordCountProcess.initalize(conf);
		wordCountProcess.start();
		
		TwitterCloudConfiguration tagCloudConf = new TwitterCloudConfiguration();
		
		tagCloudConf.setTwitterCloudMapper(TwitterTagCloudMapper.class);
		
		TwitterTagCloudProcess tagCloudProcess = new TwitterTagCloudProcess();
		tagCloudProcess.initalize(tagCloudConf);
		tagCloudProcess.start();
		
		CloudGenerator.generateCloud(cloud,"example-resources/gen");
	}
	/*
	 * ROW: <twitterHandle><UUID>
	 * COLUMN FAMILY: "LINE" // this is to stay consistent with our previous Ingest
	 * COLUMN QUALIFIER: <twitterHandle>
	 * VALUE: Tweet's text
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
