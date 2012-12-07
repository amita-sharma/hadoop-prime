package edu.american.student.process;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.american.student.conf.HadoopJobConfiguration;
import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;
import edu.american.student.foreman.HadoopForeman;

public class TwitterWordCountProcess implements PrimeProcess
{
	private Class<? extends Mapper<?,?,?,?>> mapper;
	private Class<? extends Reducer<?,?,?,?>> reducer;
	
	@SuppressWarnings("unchecked")
	@Override
	public void initalize(ProcessConfiguration conf) throws ProcessException
	{
		mapper = ((Class<? extends Mapper<?, ?, ?, ?>>) conf.getTwitterWordCountMapper());
		reducer =((Class<? extends Reducer<?, ?, ?, ?>>) conf.getTwitterWordCountReducer());
		
	}

	@Override
	public void start() throws ProcessException
	{
		HadoopForeman hForeman = new HadoopForeman();
		
		//AccumuloWordCountMapper configuration
		HadoopJobConfiguration conf = new HadoopJobConfiguration();
		conf.setJobName(HadoopJobConfiguration.buildJobName(IndexProcess.class));
		conf.setMapperClass(mapper);
		conf.setReducerClass(reducer);
		
		Collection<Pair<Text, Text>> cfPairs = new ArrayList<Pair<Text, Text>>();
		cfPairs.add(new Pair<Text, Text>(new Text("LINE"), null));
		conf.setFetchColumns(cfPairs);
		
		
		conf.setInputFormatClass(AccumuloInputFormat.class);
		conf.setOutputFormatClass(AccumuloOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setNumReduceTasks(1);
		
		hForeman.runJob(conf);

		
	}

}
