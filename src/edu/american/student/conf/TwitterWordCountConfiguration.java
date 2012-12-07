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

/**
 * A ProcessConfiguration for TwitterWordCount (part of the TwitterTagCloudProcess)
 * @author cam
 *
 */
public class TwitterWordCountConfiguration extends ProcessConfiguration
{

	/**
	 * 
	 * @param mapper The TwitterWordCountMapper
	 */
	public void setTwitterWordCountMapper(Class<?> mapper)
	{
		this.twitterWordCountMapper = mapper;
	}

	/**
	 * 
	 * @param reducer TheTwitterWordCountReducer
	 */
	public void setTwitterWordCountReducer(Class<?> reducer)
	{
		this.twitterWordCountReducer = reducer;
		
	}
	
}
