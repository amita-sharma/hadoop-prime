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

import java.io.File;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.american.student.conf.Constants;
import edu.american.student.conf.IngestConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.exception.RepositoryException;
import edu.american.student.exception.StopMapperException;
import edu.american.student.foreman.AccumuloForeman;
import edu.american.student.process.IngestProcess;
import edu.american.student.util.AccumuloAdministrator;

/**
 * Difficulty: 4 - Advanced
 * Full Explanation: FIXME
 * Relevant Files: example-resources/ingestme/
 * Uses: Hadoop 1.0.3, Accumulo 1.4.1
 * 
 * Short Description: Now that HaddopJobConfigurations are understood, they've been abstracted away within our new
 * Process Framework. See this framework in edu.american.student.process
 * 
 * Ingest means to large amounts of files quickly. They will be stored in accumulo.
 * 
 * @author cam
 *
 */
public class IngestProcessing
{

	private static String fileToProcess = "";
	private static File directoryToProcess = new File("example-resources/ingestme/");
	private static final AccumuloForeman aForeman = new AccumuloForeman();

	public static void main(String[] args) throws ProcessException
	{
		AccumuloAdministrator.setup();
		aForeman.connect();
		//walk through the ingest directory
		String[] dirContents = directoryToProcess.list();
		for(String file: dirContents)
		{
			File indexed = new File(directoryToProcess.getAbsoluteFile()+File.separator+file);
			//ingest only files that arent hidden or directories themselves
			if(!indexed.isHidden() && !indexed.isDirectory())
			{
				fileToProcess = indexed.getAbsolutePath();
				//configure an Ingest Process
				IngestConfiguration conf = new IngestConfiguration();
				conf.addFileToProcess(fileToProcess);
				conf.setIngestMapper(IngestMapper.class);
				//Run an Ingest Process
				IngestProcess ip = new IngestProcess();
				ip.initalize(conf);
				ip.start();
			}
		}
		

	}

	/**
	 * Each Mapper receives a line from a file.
	 * It adds that line to Accumulo
	 * @author cam
	 *
	 */
	public static class IngestMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>
	{
		@Override
		public void map(LongWritable key, Text val,Context context)
		{
			String table = Constants.getDefaultTable();
			String row = fileToProcess;
			String fam = "LINE";
			String qual = key.toString();
			String value = val.toString();
			try
			{
				aForeman.add(table, row, fam, qual, value);
			}
			catch (RepositoryException e)
			{
				String gripe = "Could not write to Accumulo!";
				throw new StopMapperException(gripe,e);
			}
		}
	}

}
