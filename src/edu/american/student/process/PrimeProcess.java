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
package edu.american.student.process;

import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;
/**
 * The interface for all Processes in this package.
 * @author cam
 *
 */
public interface PrimeProcess
{

	public void initalize(ProcessConfiguration conf) throws ProcessException;
	public void start() throws ProcessException;
}
