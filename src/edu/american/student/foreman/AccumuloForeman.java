/*
 * Copyright 2012 Cameron Cook Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package edu.american.student.foreman;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import edu.american.student.conf.Constants;
import edu.american.student.exception.RepositoryException;

public class AccumuloForeman
{

	private Connector conn;
	private static final Logger log = Logger.getLogger(AccumuloForeman.class.getName());

	public AccumuloForeman()
	{

	}

	@SuppressWarnings("deprecation")
	public boolean connect() throws RepositoryException
	{
		try
		{
			Instance instance = new ZooKeeperInstance(Constants.getAccumuloInstance(), Constants.getZookeeperInstance());
			conn = new Connector(instance, Constants.getAccumuloUser(), Constants.getAccumuloPassword().getBytes());
		}
		catch (AccumuloException e)
		{
			String gripe = "Could not connect the Accumulo Foreman. Check the configuration files.";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		catch (AccumuloSecurityException e)
		{
			String gripe = "Could not connect the Accumulo Foreman. Check the configuration files.";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		return true;
	}

	public Connector getConnector() throws RepositoryException
	{
		if (conn == null)
		{
			String gripe = "Could not grab the Accumulo Connector. Check Accumulo.";
			log.log(Level.SEVERE, gripe);
			throw new RepositoryException(gripe);
		}
		return conn;
	}

	public TableOperations getTableOps() throws RepositoryException
	{
		if (conn == null || conn.tableOperations() == null)
		{
			String gripe = "Could not modify tables in Accumulo. Check Accumulo.";
			log.log(Level.SEVERE, gripe);
			throw new RepositoryException(gripe);
		}
		return conn.tableOperations();
	}

	public boolean deleteTables() throws RepositoryException
	{
		Map<String, String> tableMap = this.getTableOps().tableIdMap();
		Iterator<Entry<String, String>> it = tableMap.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<String, String> entry = (it.next());
			if (!entry.getKey().startsWith("!"))
			{
				log.log(Level.INFO, "Deleting " + entry.getKey() + " ... ");
				this.deleteTable(entry.getKey());
			}
		}
		return true;
	}

	public boolean deleteTable(String name) throws RepositoryException
	{
		TableOperations tableOps = this.getTableOps();
		try
		{
			tableOps.delete(name);
		}
		catch (AccumuloException e)
		{
			String gripe = "Could not delete this table from Accumulo:" + name;
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		catch (AccumuloSecurityException e)
		{
			String gripe = "Could not delete this table from Accumulo:" + name;
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Could not delete this table from Accumulo:" + name + " (It doesn't exist)";
			log.log(Level.WARNING, gripe);

		}

		return true;
	}

	public BatchWriter getBatchWriter(String tableName) throws RepositoryException
	{
		long memBuf = 1000000L; // bytes to store before sending a batch
		long timeout = 1000L; // milliseconds to wait before sending
		int numThreads = 10;

		BatchWriter writer = null;
		try
		{
			writer = conn.createBatchWriter(tableName, memBuf, timeout, numThreads);
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Could not write to " + tableName + " (It doesn't exist)";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		return writer;
	}

	public void addBytes(String table, String row, String fam, String qual, byte[] value) throws RepositoryException
	{
		BatchWriter writer = this.getBatchWriter(table);
		Mutation m = new Mutation(row);
		Value v = new Value();
		v.set(value);
		m.put(fam, qual, new ColumnVisibility(Constants.getDefaultAuths()), System.currentTimeMillis(), v);
		try
		{
			writer.addMutation(m);
			writer.close();
		}
		catch (MutationsRejectedException e)
		{
			String gripe = "Could not assert this mutation:table=" + table + " row=" + row + " fam=" + fam;
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}

	}

	public void add(String table, String row, String fam, String qual, String value) throws RepositoryException
	{
		this.addBytes(table, row, fam, qual, value.getBytes());

	}

	public void makeTable(String tableName) throws RepositoryException
	{
		TableOperations tableOps = this.getTableOps();
		try
		{
			tableOps.create(tableName);
		}
		catch (AccumuloException e)
		{
			String gripe = "Could not create table:" + tableName;
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		catch (AccumuloSecurityException e)
		{
			String gripe = "Could not create table:" + tableName;
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}
		catch (TableExistsException e)
		{
			String gripe = "Could not create table:" + tableName + " (Table already exists)";
			log.log(Level.SEVERE, gripe, e);
			return;
		}

		log.log(Level.INFO, tableName + " created ...");

	}

	public List<Entry<Key, Value>> fetchByRowColumnFamily(String table, String row, String fam) throws RepositoryException
	{
		Authorizations auths = new Authorizations(Constants.getDefaultAuths());

		Scanner scan;
		List<Entry<Key, Value>> toRet = new ArrayList<Entry<Key, Value>>();
		try
		{
			scan = conn.createScanner(table, auths);
			scan.setRange(Range.exact(row));
			scan.fetchColumnFamily(new Text(fam));
			for (Entry<Key, Value> entry : scan)
			{
				toRet.add(entry);
			}
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Couldn't fetch columns. (Table does not exist)";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}

		return toRet;
	}

	public List<Entry<Key, Value>> fetchByColumnFamily(String table, String fam) throws RepositoryException
	{
		Authorizations auths = new Authorizations(Constants.getDefaultAuths());

		Scanner scan;
		List<Entry<Key, Value>> toRet = new ArrayList<Entry<Key, Value>>();
		try
		{
			scan = conn.createScanner(table, auths);
			scan.fetchColumnFamily(new Text(fam));
			for (Entry<Key, Value> entry : scan)
			{
				toRet.add(entry);
			}
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Couldn't fetch columns. (Table does not exist)";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}

		return toRet;
	}

	public List<Entry<Key, Value>> fetchByQualifier(String table, String fam, String qual) throws RepositoryException
	{
		Authorizations auths = new Authorizations(Constants.getDefaultAuths());
		List<Entry<Key, Value>> toRet = new ArrayList<Entry<Key, Value>>();
		Scanner scan;
		try
		{
			scan = conn.createScanner(table, auths);
			scan.fetchColumn(new Text(fam), new Text(qual));
			for (Entry<Key, Value> entry : scan)
			{
				toRet.add(entry);
			}
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Could not fetch columns. (Table:" + table + " doesn't exist)";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}

		return toRet;
	}

	public boolean tableExists(String name) throws RepositoryException
	{
		return this.getTableOps().exists(name);
	}

	private List<Entry<Key, Value>> fetchByRowColumnQualifier(String table, String row, String fam, String qualifierRegex) throws RepositoryException
	{
		Authorizations auths = new Authorizations(Constants.getDefaultAuths());

		Scanner scan;
		List<Entry<Key, Value>> toRet = new ArrayList<Entry<Key, Value>>();
		try
		{
			scan = conn.createScanner(table, auths);
			scan.setRange(Range.exact(row));
			scan.fetchColumnFamily(new Text(fam));
			for (Entry<Key, Value> entry : scan)
			{
				String qualifier = entry.getKey().getColumnQualifier().toString();
				if (qualifier.contains(qualifierRegex))
				{
					toRet.add(entry);
				}
			}
		}
		catch (TableNotFoundException e)
		{
			String gripe = "Couldn't fetch columns. (Table does not exist)";
			log.log(Level.SEVERE, gripe, e);
			throw new RepositoryException(gripe, e);
		}

		return toRet;
	}

}
