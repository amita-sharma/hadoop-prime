package edu.american.student.conf;

public class TwitterWordCountConfiguration extends ProcessConfiguration
{

	public void setTwitterWordCountMapper(Class<?> mapper)
	{
		this.twitterWordCountMapper = mapper;
	}

	public void setTwitterWordCountReducer(Class<?> reducer)
	{
		this.twitterWordCountReducer = reducer;
		
	}
	
}
