package edu.american.student.conf;

import java.io.File;

public class IngestConfiguration extends ProcessConfiguration
{

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
	
	public void addFileToProcess(String filePath)
	{
		this.ingestFilesToProcess.add(new File(filePath));
	}

	public void setIngestMapper(Class<?> mapper)
	{
		this.ingestMapper = mapper;
	}
}
