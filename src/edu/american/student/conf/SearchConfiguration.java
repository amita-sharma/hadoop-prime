package edu.american.student.conf;

public class SearchConfiguration extends ProcessConfiguration
{

	public void setSearchMapper(Class<?> searchMapper)
	{
		this.searchMapper = searchMapper;
	}
	
	public void setSearchTerm(String term)
	{
		this.searchTerm = term;
	}

}
