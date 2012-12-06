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
