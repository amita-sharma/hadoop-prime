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
package edu.american.student.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * The abstract ProcessConfiguration
 * All Configurations need to EXTEND this one
 * 
 * Everything in this class are getters() (not setters). 
 * @author cam
 *
 */
public abstract class ProcessConfiguration
{

	List<File> ingestFilesToProcess = new ArrayList<File>();
	Class<?> ingestMapper ;
	Class<?> indexMapper;
	Class<?> searchMapper;
	Class<?> twitterMapper;
	Class<?> twitterWordCountMapper;
	Class<?> twitterWordCountReducer;
	Class<?> twitterCloudMapper;
	String searchTerm;
	String ingestDefintions = "";
	
	/**
	 * 
	 * @return A list of File objects that represent the files to Ingest
	 */
	public List<File> getIngestFilesToProcess()
	{
		return ingestFilesToProcess;
	}
	
	/**
	 * @return A .class object representing the IngestMapper
	 */
	public Class<?> getIngestMapper()
	{
		return ingestMapper;
	}
	
	/**
	 * @return A .class object representing the IndexMapper
	 */
	public Class<?> getIndexMapper()
	{
		return indexMapper;
	}
	
	/**
	 * @return A .class object representing the SearchMapper
	 */
	public Class<?> getSearchMapper()
	{
		return searchMapper;
	}
	
	/**
	 * @return The term to search for. Used in SearchProcess
	 */
	public String getSearchTerm()
	{
		return this.searchTerm;
	}
	
	/**
	 * @return A file that points to the Twitter Ingest File<br>
	 * The file is a new line delimited list of twitter names to Ingest tweets
	 */
	public String getTwitterIngestDefintions()
	{
		return this.ingestDefintions;
	}
	
	/**
	 * @return A .class object representing the TwitterIngestMapper
	 */
	public Class<?> getTwitterIngestMapper()
	{
		return this.twitterMapper;
	}
	
	/**
	 * @return A .class object representing the TwitterWordCountMapper
	 */
	public Class<?> getTwitterWordCountMapper()
	{
		return this.twitterWordCountMapper;
	}
	
	/**
	 * @return A .class object representing the TwitterWordCountReducer
	 */
	public Class<?> getTwitterWordCountReducer()
	{
		return this.twitterWordCountReducer;
	}
	
	/**
	 * @return A .class object representing the TwitterTagCloudMapper
	 */
	public Class<?> getTwitterCloudMapper()
	{
		return this.twitterCloudMapper;
	}
}
