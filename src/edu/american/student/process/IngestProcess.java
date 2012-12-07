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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.foreman.HadoopForeman;

/**
 * A Hadoop prime process that launches the common Ingest Process
 * @author cam
 *
 */
public class IngestProcess implements PrimeProcess
{
	private List<File> ingestFiles = new ArrayList<File>();// list of files to process
	private Class<? extends Mapper<?,?,?,?>> mapper = null;
	
	/**
	 * Grab relevant details from the conf
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		this.ingestFiles = conf.getIngestFilesToProcess();
		this.mapper =(Class<? extends Mapper<?, ?, ?, ?>>) conf.getIngestMapper();
	}

	/**
	 * Launch the Ingest Process
	 */
	@Override
	public void start() throws ProcessException
	{
		System.out.println("Ingesting "+ingestFiles.get(0).getAbsolutePath()+" ....");
		/*
		 * TextInputFormat - Read from a File
		 * overridePathToProcess - The file path to process to read from
		 * 
		 * OutputKey, Value - Null (N/A)
		 */
		HadoopJobConfiguration entryAddConf = new HadoopJobConfiguration();
		entryAddConf.setJobName(HadoopJobConfiguration.buildJobName(IngestProcess.class));
		entryAddConf.setMapperClass(mapper);
		entryAddConf.setInputFormatClass(TextInputFormat.class);
		entryAddConf.overridePathToProcess(new Path(ingestFiles.get(0).getAbsolutePath()));
		entryAddConf.setOutputFormatClass(NullOutputFormat.class);
		entryAddConf.setOutputKeyClass(NullWritable.class);
		entryAddConf.setOutputValueClass(NullWritable.class);

		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(entryAddConf);
		System.out.println("Ingest "+ingestFiles.get(0).getAbsolutePath()+" Complete.");
		
	}
}
