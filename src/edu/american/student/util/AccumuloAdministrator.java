/* Copyright 2012 Cameron Cook
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package edu.american.student.util;

import edu.american.student.conf.Constants;
import edu.american.student.exception.RepositoryException;
import edu.american.student.foreman.AccumuloForeman;

public class AccumuloAdministrator
{

	public static void main(String[] args) throws RepositoryException
	{
		AccumuloAdministrator.setup();
	}
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
