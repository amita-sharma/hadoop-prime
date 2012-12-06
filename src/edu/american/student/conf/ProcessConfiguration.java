package edu.american.student.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class ProcessConfiguration
{

	List<File> ingestFilesToProcess = new ArrayList<File>();
	Class<?> ingestMapper ;
	Class<?> indexMapper;
	Class<?> searchMapper;
	Class<?> twitterMapper;
	String searchTerm;
	String ingestDefintions = "";
	
	public List<File> getIngestFilesToProcess()
	{
		return ingestFilesToProcess;
	}
	
	public Class<?> getIngestMapper()
	{
		return ingestMapper;
	}
	
	public Class<?> getIndexMapper()
	{
		return indexMapper;
	}
	
	public Class<?> getSearchMapper()
	{
		return searchMapper;
	}
	
	public String getSearchTerm()
	{
		return this.searchTerm;
	}
	
	public String getTwitterIngestDefintions()
	{
		return this.ingestDefintions;
	}
	
	public Class<?> getTwitterIngestMapper()
	{
		return this.twitterMapper;
	}
}
