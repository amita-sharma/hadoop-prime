package edu.american.student.conf;
/* Copyright 2012 Cameron Cook
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import java.util.Collection;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A configuration object for running a Hadoop Job
 * 
 * @author cam
 * 
 */
public class HadoopJobConfiguration
{
	/**
	 * Hadoop's job name
	 */
	private String jobName = "DefaultJobName";

	/**
	 * The mapper class to run
	 */
	private Class<? extends Mapper<?, ?, ?, ?>> mapperClass;

	/**
	 * The reducer class to run
	 */
	private Class<? extends Reducer<?, ?, ?, ?>> reducerClass;

	/**
	 * The InputFormat class of the Mapper
	 */
	private Class<?> inputClass;

	/**
	 * The OutputFormat class of the Mapper
	 */
	private Class<?> outputClass;

	/**
	 * The OutputKeyFormat class of the Mapper
	 */
	private Class<?> outputKeyClass;

	/**
	 * The OutputValueFormat class of the Mapper
	 */
	private Class<?> outputValueClass;

	/**
	 * The number of Reducer tasks
	 */
	private int numOfReduceTasks = 0;

	/**
	 * The default table to grab AccumuloInputFormat stuff from. use the
	 * override function to change this
	 * 
	 * Check mnemosyne-site.conf to change
	 */
	private String defaultTable =Constants.getDefaultTable();

	/**
	 * The default accumulo authorizations
	 * 
	 * Check mnemosyne-site.conf to change
	 */
	private Authorizations defaultAuths = new Authorizations(Constants.getDefaultAuths());

	/**
	 * If the job is using the FileInputFormat. This is the path to the file to
	 * process.
	 */
	private Path pathToProcess = null;

	/**
	 * A collection of Column Family, Column Qualifier pairs to grab from
	 * AccumuloInputFormat
	 */
	private Collection<Pair<Text, Text>> cfPairs;

	/**
	 * Returns the name of the Hadoop Job
	 * 
	 * @return the name of the Hadoop Job
	 */
	public String getJobName()
	{
		return jobName;
	}

	/**
	 * Sets the name of the Hadoop Job
	 * 
	 * @param jobName
	 */
	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}

	/**
	 * Returns the Mapper class defined by this job
	 * 
	 * @return
	 */
	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
	{
		return mapperClass;
	}

	/**
	 * Sets the Mapper class defined by this job
	 * 
	 * @param mapperClass
	 */
	public void setMapperClass(Class<? extends Mapper<?, ?, ?, ?>> mapperClass)
	{
		this.mapperClass = mapperClass;
	}

	/**
	 * Returns the InputFormat class defined by this job
	 * 
	 * @return
	 */
	public Class<?> getInputFormatClass()
	{
		return inputClass;
	}

	/**
	 * Sets the InputFormat class defined by this job
	 * 
	 * @param inputClass
	 */
	public void setInputFormatClass(Class<?> inputClass)
	{
		this.inputClass = inputClass;
	}

	/**
	 * Returns the OutputFormat class defined by this job
	 * 
	 * @return
	 */
	public Class<?> getOutputFormatClass()
	{
		return this.outputClass;
	}

	/**
	 * Sets the OutputFormat class defined by this job
	 * 
	 * @param class1
	 */
	public void setOutputFormatClass(@SuppressWarnings("rawtypes") Class class1)
	{
		this.outputClass = class1;
	}

	/**
	 * Returns the OutputKeyFormat class defined by this job
	 * 
	 * @return
	 */
	public Class<?> getOutputKeyClass()
	{
		return outputKeyClass;
	}

	/**
	 * Sets the OutputKeyFormat class defined by this job
	 * 
	 * @param outputKeyClass
	 */
	public void setOutputKeyClass(Class<?> outputKeyClass)
	{
		this.outputKeyClass = outputKeyClass;
	}

	/**
	 * Returns the OutputValueFormat class defined by this job
	 * 
	 * @return
	 */
	public Class<?> getOutputValueClass()
	{
		return outputValueClass;
	}

	/**
	 * Sets the OutputValueFormat class defined by this job
	 * 
	 * @param outputValueClass
	 */
	public void setOutputValueClass(Class<?> outputValueClass)
	{
		this.outputValueClass = outputValueClass;
	}

	/**
	 * Sets the number of Reducer tasks
	 * 
	 * @param i
	 */
	public void setNumReduceTasks(int i)
	{
		this.numOfReduceTasks = i;
	}

	/**
	 * Returns the number of Reducer tasks
	 * 
	 * @return
	 */
	public int getNumReduceTasks()
	{
		return this.numOfReduceTasks;
	}

	/**
	 * Replaces the Default Accumulo Table with this one.
	 * 
	 * @param defaultTable
	 */
	public void overrideDefaultTable(String defaultTable)
	{
		this.defaultTable = defaultTable;
	}

	/**
	 * Returns the Default Accumulo Table to use for this MR Process
	 * 
	 * @return
	 */
	public String getDefaultTable()
	{
		return this.defaultTable;
	}

	/**
	 * Sets the Default Accumulo Authorizations
	 */
	public void setDefaultAuths(Authorizations auths)
	{
		this.defaultAuths = auths;
	}

	/**
	 * Returns the Default Accumulo Authorizations
	 * 
	 * @return
	 */
	public Authorizations getDefaultAuths()
	{
		return this.defaultAuths;
	}

	/**
	 * Returns the Reducer Class for this MR Job
	 * 
	 * @return
	 */
	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
	{
		return this.reducerClass;
	}

	/**
	 * Sets the Reducer class for this MR Job
	 * 
	 * @param reducerClass
	 */
	public void setReducerClass(Class<? extends Reducer<?, ?, ?, ?>> reducerClass)
	{
		this.reducerClass = reducerClass;
	}

	/**
	 * Creates a job name with a Class
	 * 
	 * @param className
	 * @return
	 */
	public static String buildJobName(Class<?> className)
	{
		return className.getSimpleName();
	}

	/**
	 * If FileInputFormat is used, this defines which path MR should process
	 * 
	 * @param path
	 */
	public void overridePathToProcess(Path path)
	{
		this.pathToProcess = path;
	}

	/**
	 * Returns the path to process if FileInputFormat is used
	 * 
	 * @return
	 */
	public Path getPathToProcess()
	{
		return this.pathToProcess;
	}

	/**
	 * Returns the Column Family, Column Qualifier pairs that define rows to
	 * grab in Accumulo
	 * 
	 * @return
	 */
	public Collection<Pair<Text, Text>> getFetchColumns()
	{
		return cfPairs;
	}

	/**
	 * Sets the Column Family, Column Qualifer pairs that define rows to grab in
	 * Accumulo
	 * 
	 * @param pairs
	 */
	public void setFetchColumns(Collection<Pair<Text, Text>> pairs)
	{
		this.cfPairs = pairs;
	}

}
