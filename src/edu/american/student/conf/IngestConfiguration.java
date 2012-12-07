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

/**
 * A ProcessConfiguration for IngestProcess
 * @author cam
 *
 */
public class IngestConfiguration extends ProcessConfiguration
{

	/**
	 * @deprecated
	 */
	public void addDirectoryToProcess(String dir)
	{
		File d = new File(dir);
		String[] contents = d.list();
		for(String file: contents)
		{
			File toAdd = new File(d.getAbsolutePath()+File.separator+file);
			if(!toAdd.isHidden() && !toAdd.isDirectory())
			{
				addFileToProcess(toAdd.getAbsolutePath());
			}
			else if (toAdd.isDirectory())
			{
				addDirectoryToProcess(toAdd.getAbsolutePath());
			}
		}
	}
	
	/**
	 *  Add a file to be ingested
	 * @param filePath The file path of the file to be ingested
	 */
	public void addFileToProcess(String filePath)
	{
		this.ingestFilesToProcess.add(new File(filePath));
	}

	/**
	 * Sets the Ingest Mapper
	 * @param mapper The .class object to the Ingest Mapper
	 */
	public void setIngestMapper(Class<?> mapper)
	{
		this.ingestMapper = mapper;
	}
}
