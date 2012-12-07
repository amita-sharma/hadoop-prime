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

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.foreman.HadoopForeman;

public class TwitterIngestProcess implements PrimeProcess
{
	private String twitterIngestDefintions = "";
	private Class<? extends Mapper<?,?,?,?>> mapper;
	
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		twitterIngestDefintions = conf.getTwitterIngestDefintions();
		mapper = (Class<? extends Mapper<?, ?, ?, ?>>) conf.getTwitterIngestMapper();
		
	}

	@Override
	public void start() throws ProcessException
	{
		System.out.println("Ingesting "+twitterIngestDefintions+" ....");
		HadoopJobConfiguration entryAddConf = new HadoopJobConfiguration();
		entryAddConf.setJobName(HadoopJobConfiguration.buildJobName(IngestProcess.class));
		entryAddConf.setMapperClass(mapper);
		entryAddConf.setInputFormatClass(TextInputFormat.class);
		entryAddConf.overridePathToProcess(new Path(twitterIngestDefintions));
		entryAddConf.setOutputFormatClass(AccumuloOutputFormat.class);
		entryAddConf.setOutputKeyClass(Text.class);
		entryAddConf.setOutputValueClass(Mutation.class);

		HadoopForeman hForeman = new HadoopForeman();
		hForeman.runJob(entryAddConf);
		System.out.println("Ingest "+twitterIngestDefintions+" Complete.");
	}

}
