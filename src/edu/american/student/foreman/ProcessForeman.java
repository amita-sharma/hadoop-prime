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
package edu.american.student.foreman;

import edu.american.student.examples.level4.IndexProcessing;
import edu.american.student.examples.level4.IngestProcessing;
import edu.american.student.examples.level5.TwitterIngest;
import edu.american.student.exception.ProcessException;

/**
 * A Foreman for controlling your processes
 * @author cam
 *
 */
public class ProcessForeman
{

	/**
	 * Common ingest
	 * @throws ProcessException
	 */
	public static void ingest() throws ProcessException
	{
		IngestProcessing.main(null);
	}

	/**
	 * Common index
	 * @throws ProcessException
	 */
	public static void index() throws ProcessException
	{
		IndexProcessing.main(null);
		
	}

	/**
	 * Twitter Ingest
	 * @throws ProcessException
	 */
	public static void ingestTwitter() throws ProcessException
	{
		TwitterIngest.main(null);
		
	}

	/**
	 * Twitter Index
	 * @throws ProcessException
	 */
	public static void indexTwitter() throws ProcessException
	{
		IndexProcessing.main(null);
	}

}
