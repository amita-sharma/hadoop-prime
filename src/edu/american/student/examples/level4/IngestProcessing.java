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

public class IngestProcessing
{

	private static String fileToProcess = "";
	private static File directoryToProcess = new File("example-resources/ingestme/");
	private static final AccumuloForeman aForeman = new AccumuloForeman();
	/**
	 * @param args
	 * @throws ProcessException 
	 */
	public static void main(String[] args) throws ProcessException
	{
		AccumuloAdministrator.setup();
		aForeman.connect();
		String[] dirContents = directoryToProcess.list();
		for(String file: dirContents)
		{
			File indexed = new File(directoryToProcess.getAbsoluteFile()+File.separator+file);
			if(!indexed.isHidden() && !indexed.isDirectory())
			{
				fileToProcess = indexed.getAbsolutePath();
				
				IngestConfiguration conf = new IngestConfiguration();
				conf.addFileToProcess(fileToProcess);
				conf.setIngestMapper(IngestMapper.class);
				
				IngestProcess ip = new IngestProcess();
				ip.initalize(conf);
				ip.start();
			}
		}
		

	}
	
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
