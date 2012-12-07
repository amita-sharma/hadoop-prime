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

public class TwitterTagCloudProcess implements PrimeProcess
{
	private Class<? extends Mapper<?,?,?,?>> mapper;
	
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		mapper = ((Class<? extends Mapper<?, ?, ?, ?>>) conf.getTwitterCloudMapper());

	}

	@Override
	public void start() throws ProcessException
	{
		HadoopForeman hForeman = new HadoopForeman();

		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(IndexProcess.class));
		conf.setMapperClass(mapper);

		Collection<Pair<Text, Text>> cfPairs = new ArrayList<Pair<Text, Text>>();
		cfPairs.add(new Pair<Text, Text>(new Text("TAG"), null));
		conf.setFetchColumns(cfPairs);

		conf.setInputFormatClass(AccumuloInputFormat.class);
		conf.setOutputFormatClass(NullOutputFormat.class);

		hForeman.runJob(conf);

	}

}
