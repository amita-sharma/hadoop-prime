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
package edu.american.student.exception;

/**
 * Thrown when something goes awry with Accumulo
 * @author cam
 *
 */
public abstract class DataspaceException extends ProcessException
{

	public DataspaceException(String message)
	{
		super(message);
	}

	public DataspaceException(String message, Throwable throwable)
	{
		super(message,throwable);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2336933304983333809L;

}
