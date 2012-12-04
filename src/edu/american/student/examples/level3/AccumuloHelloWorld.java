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

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.exception.HadoopException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.foreman.HadoopForeman;

/**
 * Difficulty: 3 - Intermediate
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
public class AccumuloHelloWorld
{

	private static final AccumuloForeman aForeman = new AccumuloForeman();
	
	public static void main(String[] args) throws HadoopException, RepositoryException
	{
		aForeman.connect();
		HadoopForeman hForeman = new HadoopForeman();
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(AccumuloHelloWorld.class));
		// conf.setMapperClass(BaseNetworkBuilderMapper.class);
		// conf.overrideDefaultTable(AccumuloForeman.getArtifactRepositoryName());
		Collection<Pair<Text, Text>> cfPairs = new ArrayList<Pair<Text, Text>>();
		// cfPairs.add(new Pair<Text, Text>(new Text(artifact.getArtifactId()), null));
		conf.setFetchColumns(cfPairs);
		conf.setInputFormatClass(AccumuloInputFormat.class);
		conf.setOutputFormatClass(AccumuloOutputFormat.class);
		hForeman.runJob(conf);
	}
	
	public static class AccumuloHelloWorldMapper extends Mapper<Key, Value, Writable, Writable>
	{
	
	}
}
