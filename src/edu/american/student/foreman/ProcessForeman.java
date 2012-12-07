package edu.american.student.foreman;

import edu.american.student.examples.level4.IndexProcessing;
import edu.american.student.examples.level4.IngestProcessing;
import edu.american.student.examples.level5.TwitterIngest;
import edu.american.student.exception.ProcessException;

public class ProcessForeman
{

	public static void ingest() throws ProcessException
	{
		IngestProcessing.main(null);
	}

	public static void index() throws ProcessException
	{
		IndexProcessing.main(null);
		
	}

	public static void ingestTwitter() throws ProcessException
	{
		TwitterIngest.main(null);
		
	}

	public static void indexTwitter() throws ProcessException
	{
		IndexProcessing.main(null);
		
	}

}
