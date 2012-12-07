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
package edu.american.student.process;

import java.util.ArrayList;

import java.util.Collection;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.foreman.HadoopForeman;
/**
 * A Hadoop prime process that launches the Twitter Tag Cloud Process
 * @author cam
 *
 */
public class TwitterTagCloudProcess implements PrimeProcess
{
	private Class<? extends Mapper<?,?,?,?>> mapper;
	
	/**
	 * Grab relevant details for Twitter Tag Cloud Process
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		mapper = ((Class<? extends Mapper<?, ?, ?, ?>>) conf.getTwitterCloudMapper());

	}

	/**
	 * Start the Twitter Tag Cloud Process
	 */
	@Override
	public void start() throws ProcessException
	{
		HadoopForeman hForeman = new HadoopForeman();

		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(IndexProcess.class));
		conf.setMapperClass(mapper);

		/*
		 * Grab all the Accumulo entries with the Column Family: TAG 
		 */
		Collection<Pair<Text, Text>> cfPairs = new ArrayList<Pair<Text, Text>>();
		cfPairs.add(new Pair<Text, Text>(new Text("TAG"), null));
		conf.setFetchColumns(cfPairs);

		conf.setInputFormatClass(AccumuloInputFormat.class);
		conf.setOutputFormatClass(NullOutputFormat.class);

		hForeman.runJob(conf);

	}

}
