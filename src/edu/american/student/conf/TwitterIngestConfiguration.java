package edu.american.student.conf;

public class TwitterIngestConfiguration extends ProcessConfiguration
{	
	public void setTwitterIngestDefinitions(String def)
	{
		this.ingestDefintions = def;
	}
	
	public void setTwitterIngestMapper(Class<?> mapper)
	{
		this.twitterMapper = mapper;
	}

}
