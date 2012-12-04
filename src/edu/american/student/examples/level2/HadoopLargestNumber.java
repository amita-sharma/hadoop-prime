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
package edu.american.student.examples.level2;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
 * Full Explanation: FIXME
 * Relevant Files: example-resources/sortme.txt
 * Uses: Hadoop 1.0.3
 * 
 * Short Description: Each node in the cluster will print out the piece it got.
 * The key is what Hadoop considers the line number. The value is the text at that position.
 * The Mapper will break down the line into several numbers and sent lineNumber:number pairs to the Reducer.
 * 
 * The Reducer then will grab all the key-values with the same key (same line number), then sort the list. 
 * Printing out the largest value in the list per line
 * @author cam
 *
 */
public class HadoopLargestNumber
{

	public static void main(String[] args) throws HadoopException
	{
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(HadoopLargestNumber.class));
		conf.setMapperClass(HadoopLargestNumberMapper.class);
		conf.setReducerClass(HadoopLargestNumberReducer.class);
		conf.setInputFormatClass(TextInputFormat.class);
		conf.overridePathToProcess(new Path("example-resources/sortme.txt"));

		
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(1);

		// Ask the Hadoop Foreman to schedule this job
		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(conf);
		System.out.println("Hadoop Job Complete.");
	}

	public static class HadoopLargestNumberMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
	{
		@Override
		public void map(LongWritable ik, Text iv, Context context)
		{
			long lineNumber = ik.get();
			String line = iv.toString();
			System.out.println(this.getClass().getSimpleName()+": key = "+lineNumber+" value = "+line);
			//grab each individual integer on the line.
			//they are comma separated
			String[] numbersAsStrings = line.split(",");
			
			for(String number: numbersAsStrings)
			{
				IntWritable keyToSend = new IntWritable((int)lineNumber);
				IntWritable valueToSend = new IntWritable(Integer.parseInt(number));
				try
				{
					context.write(keyToSend, valueToSend);
				}
				catch (Exception e)
				{
					String gripe = "Could not write to context!";
					throw new StopMapperException(gripe,e);
				}

			}
			
		}
	}

	public static class HadoopLargestNumberReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
	{
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		{
			int lineNumber = key.get();
			int maxValue = Integer.MIN_VALUE;
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext())
			{
				int indexedNumber = iterator.next().get();
				if(indexedNumber > maxValue)
				{
					maxValue = indexedNumber;
				}
			}
			System.out.println("Line #:"+lineNumber+" Max Value:"+maxValue);
		}
	}

}
