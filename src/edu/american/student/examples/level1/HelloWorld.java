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
package edu.american.student.examples.level1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.exception.HadoopException;
import edu.american.student.foreman.HadoopForeman;

/**
 * Start Here
 * 
 * Difficulty: 1 - Simple
 * Full Explanation: FIXME
 * Relevant Files: example-resources/hello_world.txt
 * Uses: Hadoop 1.0.3
 * 
 * Short Description: Each node in the cluster will receive a part of the file in example-resources/hello_world.txt. 
 * Each node in the cluster will print out the piece it got. The piece is denoted as a key and a value. 
 * The key is what Hadoop considers the line number. The value is the text at that position.
 * @author cam
 *
 */
public class HelloWorld
{
	public static void main(String[] args) throws HadoopException
	{
		HadoopJobConfiguration conf = new HadoopJobConfiguration();//schedule a Hadoop Job
		conf.setJobName(HadoopJobConfiguration.buildJobName(HelloWorld.class));	//Tell Hadoop who requested this job
		conf.setMapperClass(HelloWorldMapper.class);//Tell Hadoop where to find the Mapper class (where each node will run said mapper)
		conf.setInputFormatClass(TextInputFormat.class);//Tell Hadoop what input it should be expecting (a text file)
		conf.overridePathToProcess(new Path("example-resources/hello_world.txt"));//In the case of TextInputForemat, you have to tell Hadoop where to look for the file
		
		//These are for the Reducer (which does not exist in this example). These are just default values
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		//Ask the Hadoop Foreman to schedule this job
		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(conf);
	}
	
	/**
	 * This is a Mapper. Each node on the Hadoop cluster will run this Mapper in parallel.
	 * 
	 * The map() function will recieve a key-value as a LongWritable and a Text pair.
	 * The Mapper is setup to send a key-value pair to a Reducer as Text,IntWritable, but it is not in use
	 * @author cam
	 *
	 */
	public static class HelloWorldMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		
		@Override
		public void map(LongWritable ik, Text iv, Context context)
		{
			long key = ik.get(); //We have a key
			String value = iv.toString(); // We have a value
			System.out.println(this.getClass().getSimpleName()+": key = "+key+" value = "+value);//Print the key and value
		}
	}

}
