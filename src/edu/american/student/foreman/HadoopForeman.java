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
package edu.american.student.foreman;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.exception.HadoopException;
import edu.american.student.conf.Constants;
/**
 * A Foreman which controls Hadoop Jobs
 * @author cam
 *
 */
public class HadoopForeman
{
	private final static Logger log = Logger.getLogger(HadoopForeman.class.getName());

	/**
	 * Creates a native hadoop job given a HadoopJobConfiguration
	 * @param conf
	 * @return
	 * @throws HadoopException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Job getHadoopJob(HadoopJobConfiguration conf) throws HadoopException
	{
		Job job;
		try
		{
			job = new Job();
			job.setJobName(conf.getJobName());

			job.setMapperClass(conf.getMapperClass());
			job.setInputFormatClass((Class<? extends InputFormat>) conf.getInputFormatClass());

			if (conf.getOutputFormatClass() != null)
			{
				job.setOutputFormatClass((Class<? extends OutputFormat>) conf.getOutputFormatClass());
			}
			if (conf.getOutputKeyClass() != null)
			{
				job.setOutputKeyClass(conf.getOutputKeyClass());
			}
			if (conf.getOutputValueClass() != null)
			{
				job.setOutputValueClass(conf.getOutputValueClass());
			}
			if (conf.getReducerClass() != null)
			{
				job.setReducerClass(conf.getReducerClass());
			}

			job.setNumReduceTasks(conf.getNumReduceTasks());
			Configuration conf1 = job.getConfiguration();
			if (conf.getInputFormatClass() == AccumuloInputFormat.class)
			{
				AccumuloInputFormat.setInputInfo(conf1, Constants.getAccumuloUser(), Constants.getAccumuloPassword().getBytes(), conf.getDefaultTable(), conf.getDefaultAuths());
				AccumuloInputFormat.setZooKeeperInstance(conf1, Constants.getZookeeperInstanceName(), Constants.getZookeeperInstance());

			}
			if (conf.getFetchColumns() != null)
			{
				AccumuloInputFormat.fetchColumns(conf1, conf.getFetchColumns());
			}
			else if (conf.getInputFormatClass() == TextInputFormat.class)
			{
				if (conf.getPathToProcess() != null)
				{
					FileInputFormat.setInputPaths(job, conf.getPathToProcess());
				}
			}
			if (conf.getOutputFormatClass() == AccumuloOutputFormat.class)
			{
				AccumuloOutputFormat.setOutputInfo(conf1,Constants.getAccumuloUser(), Constants.getAccumuloPassword().getBytes(), true, conf.getDefaultTable());
				AccumuloOutputFormat.setZooKeeperInstance(conf1, Constants.getZookeeperInstanceName(), Constants.getZookeeperInstance());

			}
			return job;

		}
		catch (IOException e)
		{
			String gripe = "Could not configure a Hadoop job";
			log.log(Level.SEVERE, gripe, e);
			throw new HadoopException(gripe, e);

		}

	}

	/**
	 * Runs a Hadoop job!
	 * @param hConfig
	 * @return
	 * @throws HadoopException
	 */
	public boolean runJob(HadoopJobConfiguration hConfig) throws HadoopException
	{
		try
		{
			return this.getHadoopJob(hConfig).waitForCompletion(true);
		}
		catch (IOException e)
		{
			String gripe = "Could not start a Hadoop job";
			log.log(Level.SEVERE, gripe, e);
			throw new HadoopException(gripe, e);
		}
		catch (InterruptedException e)
		{
			String gripe = "Could not start a Hadoop job";
			log.log(Level.SEVERE, gripe, e);
			throw new HadoopException(gripe, e);
		}
		catch (ClassNotFoundException e)
		{
			String gripe = "Could not start a Hadoop job";
			log.log(Level.SEVERE, gripe, e);
			throw new HadoopException(gripe, e);
		}

	}
}
