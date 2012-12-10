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
 * Full Explanation: FIXME
 * Relevant Files: example-resources/sortme.txt
 * Uses: Hadoop 1.0.3
 * 
 * Short Description: Each node in the cluster will print out the piece it got. 
 * The key is what Hadoop considers the line number. The value is the text at that position.
 * The Mapper will break down the line into several numbers and send [lineNumber:number] pairs to the Reducer.
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
		//Create a Job Configuration 
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(HadoopLargestNumber.class));
		conf.setMapperClass(HadoopLargestNumberMapper.class);
		conf.setReducerClass(HadoopLargestNumberReducer.class);
		conf.setInputFormatClass(TextInputFormat.class);
		conf.overridePathToProcess(new Path("example-resources/sortme.txt"));

		/*This time, we have a Reducer. It doesn't write files or anything,
		 * So NullOutputFormat is what we choose.
		 * The Reducer need to know what kind of key-value pairs it should expect.
		 * Because we are passing it a line number as a key, its KeyClass is IntWritable
		 * Each line number will have several numbers that have been parsed out of the line
		 * So each value passed to a single reducer will also be an integer, its ValueClass is IntWritable
		 */
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(1); //Tell Hadoop that a Reducer exists

		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(conf);
		System.out.println("Hadoop Job Complete.");
	}

	/**
	 * Each Mapper's responsibility is to grab a line from the file and parse it into several numbers.
	 * Each line contains a comma delimited set of numbers.
	 * Then the Mapper sends these numbers to the reducer
	 * @author cam
	 *
	 */
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
					//the context is simply an interface to the Reducer
					context.write(keyToSend, valueToSend);// sends [lineNumber: individualNumberParsed] to the reducer
				}
				catch (Exception e)// Oh no!
				{
					String gripe = "Could not write to context!";
					throw new StopMapperException(gripe,e); // A StopMapperException is analogous to a RuntimeException
															 // Hadoop will cease processing
				}

			}
			
		}
	}

	/**
	 * This defines our Reducer. Notice what is inbetween the <>.
	 * The order says, I Should expect an IntWritable key and (several) IntWritable values
	 * The remaining pair says that each Reducer will not write to files, or send their results elsewhere
	 * @author cam
	 *
	 */
	public static class HadoopLargestNumberReducer extends Reducer<IntWritable, IntWritable, NullWritable, NullWritable>
	{
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		{
			//its given a line number, and a list of numbers from that line
			int lineNumber = key.get();
			int maxValue = Integer.MIN_VALUE;
			Iterator<IntWritable> iterator = values.iterator(); //Don't be freaked out by this, its essentially a list of values
			while(iterator.hasNext()) // while there are more values in the list
			{
				int indexedNumber = iterator.next().get(); //grab the next value
				if(indexedNumber > maxValue)//compare
				{
					maxValue = indexedNumber;
				}
			}
			//print out largest number by line number
			System.out.println("Line #:"+lineNumber+" Max Value:"+maxValue);
		}
	}

}
