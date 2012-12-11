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
package edu.american.student.examples.level3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.Constants;
import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.examples.level2.HadoopWordCount.HadoopWordCountReducer;
import edu.american.student.exception.HadoopException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.foreman.HadoopForeman;
import edu.american.student.util.AccumuloAdministrator;

/**
 * Difficulty: 3 - Intermediate
 * Full Explanation: FIXME
 * Relevant Files: example-resources/alice.txt
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: AccumuloEntryAdderMapper will add each line of alice.txt into the default Accumulo Table
 * AccumuloWordCountMapper will iterate over those entries in Accumulo and send instances of each word per entry (therefore, per line)
 * We reuse a reducer from HadoopWordCount to print the total word counts
 * 
 * An entry in the default table of accumulo for each mapper is created;
 * ROW= <UUID>
 * COLUMN FAMILY= "LINE"
 * COLUMN QUALIFIER= <Line Number>
 * VALUE= <line contents>
 * @author cam
 *
 */
public class AccumuloWordCount
{

	private static final AccumuloForeman aForeman = new AccumuloForeman();

	public static void main(String[] args) throws RepositoryException, HadoopException
	{
		//Clean the default accumulo table
		AccumuloAdministrator.setup();
		aForeman.connect();

		// AccumuloEntryAdderMapper Conf
		HadoopJobConfiguration entryAddConf = new HadoopJobConfiguration();
		entryAddConf.setJobName(HadoopJobConfiguration.buildJobName(AccumuloHelloWorld.class) + AccumuloEntryAdderMapper.class.getName());
		entryAddConf.setMapperClass(AccumuloEntryAdderMapper.class);
		entryAddConf.setInputFormatClass(TextInputFormat.class);
		entryAddConf.overridePathToProcess(new Path("example-resources/alice.txt"));
		entryAddConf.setOutputFormatClass(NullOutputFormat.class);
		entryAddConf.setOutputKeyClass(NullWritable.class);
		entryAddConf.setOutputValueClass(NullWritable.class);

		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(entryAddConf);
		
		//AccumuloWordCountMapper configuration
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(AccumuloHelloWorld.class) + AccumuloWordCountMapper.class.getName());
		conf.setMapperClass(AccumuloWordCountMapper.class);
		conf.setReducerClass(HadoopWordCountReducer.class); //Reuse a previous Reducer
		
		/*
		 * Below we are telling Accumulo which entries to look over.
		 * Grab every entry with the Column Family: LINE
		 * and grab every Column Qualifier from those entries (hence null)
		 */
		Collection<Pair<Text, Text>> cfPairs = new ArrayList<Pair<Text, Text>>();
		cfPairs.add(new Pair<Text, Text>(new Text("LINE"), null));
		conf.setFetchColumns(cfPairs);
		
		
		conf.setInputFormatClass(AccumuloInputFormat.class);
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(1);
		
		hForeman.runJob(conf);
	}

	/**
	 * Each Mapper will receive a line of alice.txt.
	 * Add an entry to Accumulo
	 * 
	 * ROW= <UUID>
	 * COLUMN FAMILY= "LINE"
	 * COLUMN QUALIFIER= <Line Number>
	 * VALUE= <line contents>
	 * @author cam
	 *
	 */
	public static class AccumuloEntryAdderMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>
	{
		@Override
		public void map(LongWritable key, Text value, Context context)
		{
			int lineNumber = (int) key.get();
			String line = value.toString();
			String table = Constants.DEFAULT_TABLE.getName();
			String row = UUID.randomUUID().toString();
			String fam = "LINE";
			String qual = lineNumber + "";

			try
			{
				// MAIN <UUID> LINE:<lineNumber> <lineContents>
				aForeman.add(table, row, fam, qual, line);
			}
			catch (RepositoryException e)
			{
				String gripe = "Failed to write to Accumulo!";
				throw new StopMapperException(gripe, e);
			}
		}
	}

	/**
	 * Each Mapper will receive a single entry in Accumulo.
	 * That entry's value is a line from alice.txt
	 * Use the same logic to count the number of instances per word as we did previously
	 * @author cam
	 *
	 */
	public static class AccumuloWordCountMapper extends Mapper<Key, Value,Text,IntWritable>
	{
		@Override
		public void map(Key ik, Value iv, Context context)
		{
			//Key ik represents ROW:COLUMN FAMILY:COLUMN QUALIFER:AUTHORIZATIONS
			//VALUE iv represents the data held there in Accumulo
			String value = iv.toString();
			String[] words = value.split(" ");
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
			System.out.println(dictionaryIterator.hasNext());
			while (dictionaryIterator.hasNext())
			{
				Entry<String, Integer> entry = dictionaryIterator.next();
				String word = entry.getKey();
				int numberOfInstances = entry.getValue();
				try
				{
					Text key = new Text(word);
					IntWritable val = new IntWritable(numberOfInstances);
					context.write(key, val);
				}
				catch (Exception e)
				{
					String gripe = "Couldn't write to context!";
					throw new StopMapperException(gripe, e);
				}
			}
		}
	}

}
