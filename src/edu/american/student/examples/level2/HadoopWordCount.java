/*
 * This file is apart of Hadoop-prime Hadoop-prime is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version. Hadoop-prime is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Hadoop-prime. If not, see <http://www.gnu.org/licenses/>. Copyright 2012 Cameron Cook
 */
package edu.american.student.examples.level2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.exception.HadoopException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.HadoopForeman;

/**
 * Difficulty: 2 - Beginner 
 * 
 * Full Explanation: FIXME 
 * Relevant Files: example-resources/alice.txt Uses: Hadoop 1.0.3 
 * 
 * Short Description: Each Mapper will take a line of test from alice.txt. 
 * It will deconstruct it into terms, count how many times that word happens in the line 
 * 
 * It passes the Reducer key-value pairs that are word:numberOfLineInstances. 
 * The Reducer will add up the total instances of each word and print
 * 
 * @author cam
 */
public class HadoopWordCount
{

	public static void main(String[] args) throws HadoopException
	{
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(HadoopWordCount.class));

		conf.setMapperClass(HadoopWordCountMapper.class);
		conf.setReducerClass(HadoopWordCountReducer.class);

		conf.setInputFormatClass(TextInputFormat.class);
		conf.overridePathToProcess(new Path("example-resources/alice.txt"));

		// The Reducer will receive
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(1);

		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(conf);
		System.out.println("Hadoop Job Complete.");

	}

	/**
	 * Each Mapper's responsibility is to deconstruct a line into words and the number of instances
	 * 
	 * @author cam
	 */
	public static class HadoopWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		@Override
		public void map(LongWritable ik, Text iv, Context context)
		{
			@SuppressWarnings("unused")
			long lineNumber = ik.get();
			String line = iv.toString();

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
					Text key = new Text(word);
					IntWritable value = new IntWritable(numberOfInstances);
					context.write(key, value);
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
	 * Each Reducers responsibility is to count the total number of instances of each word and print
	 * 
	 * @author cam
	 */
	public static class HadoopWordCountReducer extends Reducer<Text, IntWritable, NullWritable, NullWritable>
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
		}
	}

}
