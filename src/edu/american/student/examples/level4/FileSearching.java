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

import edu.american.student.conf.SearchConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.foreman.ProcessForeman;
import edu.american.student.process.SearchProcess;

/**
 * Difficulty: 4 - Advanced
 * Full Explanation: FIXME
 * Relevant Files: example-resources/ingestme/
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: After every file is indexed, We do a search for a given search term.
 * The mapper prints out where it found the file
 * 
 * @author cam
 *
 */
public class FileSearching
{

	public static void main(String[] args) throws ProcessException
	{
		ProcessForeman.index();
		
		SearchConfiguration conf = new SearchConfiguration();
		conf.setSearchMapper(SearchMapper.class);
		//We are searching for "we"
		conf.setSearchTerm("we");
		SearchProcess process = new SearchProcess();
		process.initalize(conf);
		process.start();
		
		
	}
	/**
	 * Each mapper is given an entry where the CF:CQ pairs are INDEX:<search term>
	 * The rows of these entries are their filenames
	 * @author cam
	 *
	 */
	public static class SearchMapper extends Mapper<Key,Value,NullWritable,NullWritable>
	{
		@Override
		public void map(Key key, Value val, Context context)
		{
			String term = key.getColumnQualifier().toString();
			String file = key.getRow().toString();
			
			System.out.println("Found "+term+" in file:"+file);
		}
	}
}
