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

public class IngestProcess implements PrimeProcess
{
	private List<File> ingestFiles = new ArrayList<File>();
	private Class<? extends Mapper<?,?,?,?>> mapper = null;
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		this.ingestFiles = conf.getIngestFilesToProcess();
		this.mapper =(Class<? extends Mapper<?, ?, ?, ?>>) conf.getIngestMapper();
	}

	@Override
	public void start() throws ProcessException
	{
		System.out.println("Ingesting "+ingestFiles.get(0).getAbsolutePath()+" ....");
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
