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
package edu.american.student.util;

import edu.american.student.conf.Constants;
import edu.american.student.exception.RepositoryException;
import edu.american.student.foreman.AccumuloForeman;

/**
 * A way to clean and clear Accumulo
 * @author cam
 *
 */
public class AccumuloAdministrator
{

	public static void main(String[] args) throws RepositoryException
	{
		AccumuloAdministrator.setup();
	}
	
	/**
	 * Clean and Clear Accumulo
	 * @throws RepositoryException
	 */
	public static void setup() throws RepositoryException
	{
		System.out.println("Starting Accumulo Setup");
		AccumuloForeman aForeman = new AccumuloForeman();
		aForeman.connect();
		aForeman.deleteTables();
		aForeman.makeTable(Constants.DEFAULT_TABLE.getName());
		aForeman.setAuths(Constants.getDefaultAuths());
	}
}
