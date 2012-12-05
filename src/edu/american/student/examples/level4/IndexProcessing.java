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
package edu.american.student.examples.level4;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.american.student.conf.Constants;
import edu.american.student.conf.IndexConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.foreman.ProcessForeman;
import edu.american.student.process.IndexProcess;

/**
 * Difficulty: 4 - Advanced
 * Full Explanation: FIXME
 * Relevant Files: example-resources/ingestme/
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: Using the process framework, all the files that were once ingested,
 * every word of that file is now indexed (to be searched later)
 * 
 * @author cam
 *
 */
public class IndexProcessing
{
	private static AccumuloForeman aForeman = new AccumuloForeman();

	public static void main(String[] args) throws ProcessException
	{
		ProcessForeman.ingest(); // Call Ingest
		aForeman.connect();
		
		IndexConfiguration conf = new IndexConfiguration();
		conf.setIndexMapper(IndexMapper.class);
		IndexProcess index = new IndexProcess();
		index.initalize(conf);
		index.start();
	}
	
	/**
	 * 
	 * Each Mapper receives an entry from Accumulo.
	 * This entry is a line from a ingested file.
	 * 
	 * Each word from each line is given an INDEX entry
	 * ROW: File path
	 * COLUMN FAMILY: INDEX
	 * COLUMN QUALIFIER: <word>
	 * VALUE: <empty>
	 *
	 */
	public static class IndexMapper extends Mapper<Key,Value, NullWritable, NullWritable>
	{
		@Override
		public void map(Key key, Value value, Context context)
		{
			String orginatingFile = key.getRow().toString();
			String[] words = value.toString().split(" ");
			for(String word: words)
			{
				try
				{
					aForeman.add(Constants.getDefaultTable(), orginatingFile, "INDEX", word.toLowerCase(), "");
				}
				catch (RepositoryException e)
				{
					String gripe = "Could not write to Accumulo!";
					throw new StopMapperException(gripe,e);
				}
			}
			
		}
	}

}
