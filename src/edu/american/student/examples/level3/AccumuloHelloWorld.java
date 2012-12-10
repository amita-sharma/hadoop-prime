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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.Constants;
import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.exception.HadoopException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.foreman.HadoopForeman;
import edu.american.student.util.AccumuloAdministrator;

/**
 * Difficulty: 3 - Intermediate
 * Full Explanation: FIXME
 * Relevant Files: example-resources/hello_world.txt
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: Each Mapper will grab a line of text from hello_world.txt.
 * It calculates the first character, and the length of the line.
 * 
 * An entry in the default table of accumulo for each mapper is created;
 * ROW= <Line Number>
 * COLUMN FAMILY= "LINE"
 * COLUMN QUALIFIER= <First Character>
 * VALUE= <line length>
 * @author cam
 *
 */
public class AccumuloHelloWorld
{

	private static final AccumuloForeman aForeman = new AccumuloForeman();
	
	public static void main(String[] args) throws HadoopException, RepositoryException
	{
		//Connect to the Accumulo Foreman
		aForeman.connect();
		AccumuloAdministrator.setup();
		
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(AccumuloHelloWorld.class));	
		conf.setMapperClass(AccumuloHelloWorldMapper.class);
		conf.setInputFormatClass(TextInputFormat.class);
		conf.overridePathToProcess(new Path("example-resources/hello_world.txt"));
		
	
		conf.setOutputFormatClass(NullOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(conf);
	}
	
	/**
	 * Every Mapper gets a line of text, it then does simple calculation and inserts a row into the default table
	 * @author cam
	 *
	 */
	public static class AccumuloHelloWorldMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		@Override
		public void map(LongWritable ik, Text iv, Context context)
		{
			long lineNumber = ik.get(); 
			String data = iv.toString(); 
			

			String row = lineNumber+"";
			String fam = "LINE";
			String qual = data.charAt(0)+"";
			String value = data.length()+"";
			//Add an entry for every line in the default accumulo table
			// ROW:lineNumber LINE:firstCharacter VALUE:lineLength
			
			try
			{
				System.out.println("Writing: "+row+" | "+fam+":"+qual+" "+value);
				aForeman.add(Constants.DEFAULT_TABLE.getName(), row, fam, qual, value);
			}
			catch (RepositoryException e)
			{
				String gripe = "Couldn't write to accumulo!";
				throw new StopMapperException(gripe,e);
			}
		}
	}
	
}
