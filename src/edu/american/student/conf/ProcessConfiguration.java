package edu.american.student.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class ProcessConfiguration
{

	List<File> ingestFilesToProcess = new ArrayList<File>();
	Class<?> ingestMapper ;
	
	public List<File> getIngestFilesToProcess()
	{
		return ingestFilesToProcess;
	}
	
	public Class<?> getIngestMapper()
	{
		return ingestMapper;
	}
}
