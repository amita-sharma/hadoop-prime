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
 * These are some common Constants used in the code
 * 
 * Edit them as they suit you.
 * @author cam
 *
 */
public enum Constants
{
	/*
	 * This is the name of the default Accumulo Table.
	 * Name it something that suits you better, if you wish
	 */
	DEFAULT_TABLE("MAIN"),
	
	/*
	 * These are default authorizations to read/write to Accumulo.
	 * 
	 * Public is pretty standard.
	 * While you could change it, you should know what youre doing
	 */
	DEFAULT_AUTHS("public"),
	
	/*
	 * This is the default user for Accumulo.
	 * 
	 *  root is the default
	 */
	DEFAULT_ACCUMULO_USER("root"),
	
	/*
	 * This is the default password for the default Accumulo user.
	 * 
	 * You should probably change this for sake of security, but yeah, whatever.
	 */
	DEFAULT_ACCUMULO_PASSWORD("pass"),
	
	/*
	 * The name of the Accumulo Instance
	 * 
	 * You can get this by logging into the Accumulo shell
	 * 
	 * ./opt/accumulo/bin/accumulo shell -u root -p pass
	 */
	DEFAULT_ACCUMULO_INSTANCE("mnemosyne"),
	
	/*
	 * The instance name (not the instance itself) of zookeeper
	 */
	DEFAULT_ZOOKEEPER_INSTANCE_NAME("mnemosyne"),
	
	/*
	 * The instance itself, it  should be an IP.
	 */
	DEFAULT_ZOOKEEPER_INSTANCE("127.0.0.1"), 
	
	/*
	 * A Twitter OAUTH Consumer key.
	 * 
	 * You get this from https://dev.twitter.com/
	 * See:https://github.com/Ccook/hadoop-prime/wiki/Level-5:-Expert-:-TwitterIngest
	 */
	TWITTER_OAUTH_CONSUMER_KEY("**********"),
	
	/*
	 * A Twitter OAUTH Consumer Secret
	 * 
	 * You get this from https://dev.twitter.com/
	 * See:https://github.com/Ccook/hadoop-prime/wiki/Level-5:-Expert-:-TwitterIngest
	 */
	TWITTER_OAUTH_CONSUMER_SECRET("**********"),
	
	/*
	 * A Twitter OAUTH Access Token
	 * 
	 * You get this from https://dev.twitter.com/
	 * See:https://github.com/Ccook/hadoop-prime/wiki/Level-5:-Expert-:-TwitterIngest
	 */
	TWITTER_OAUTH_ACCESS_TOKEN("*********************"), 
	
	/*
	 * A Twitter OAUTH Access Token Secret
	 * 
	 * You get this from https://dev.twitter.com/
	 * See: https://github.com/Ccook/hadoop-prime/wiki/Level-5:-Expert-:-TwitterIngest
	 */
	TWITTER_OAUTH_ACESS_TOKEN_SECRET("***********************");

	private String name = "";

	Constants(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return this.name;
	}

	/**
	 * @return The default Accumulo table
	 */
	public static String getDefaultTable()
	{
		return DEFAULT_TABLE.getName();
	}

	/**
	 * @return The default Accumulo read/write authorizations
	 */
	public static String getDefaultAuths()
	{
		return DEFAULT_AUTHS.getName();
	}

	/**
	 * @return The default Accumulo user (usually root)
	 */
	public static String getAccumuloUser()
	{
		return DEFAULT_ACCUMULO_USER.getName();
	}

	/**
	 * @return The default Accumulo user's password
	 */
	public static String getAccumuloPassword()
	{
		return DEFAULT_ACCUMULO_PASSWORD.getName();
	}

	/**
	 * @return The name of the zookeeper instance
	 */
	public static String getZookeeperInstanceName()
	{
		return DEFAULT_ZOOKEEPER_INSTANCE_NAME.getName();
	}

	/**
	 * @return The IP address of the zookeeper instance
	 */
	public static String getZookeeperInstance()
	{
		return DEFAULT_ZOOKEEPER_INSTANCE.getName();
	}

	/**
	 * @return The name of the Accumulo instance
	 */
	public static String getAccumuloInstance()
	{
		return DEFAULT_ACCUMULO_INSTANCE.getName();
	}

	/**
	 * @return A Twitter OAuth key (You have to set this in Constants)
	 */
	public static String getTwitterOAuthConsumerKey()
	{
		return TWITTER_OAUTH_CONSUMER_KEY.getName();
	}

	/**
	 * @return A Twitter OAuth Consumer Secret (You have to set this in Constants)
	 */
	public static String getTwitterOAuthConsumerSecret()
	{
		return TWITTER_OAUTH_CONSUMER_SECRET.getName();
	}

	/**
	 * @return A Twitter OAuth Access Token (You have to set this in Constants)
	 */
	public static String getTwitterOAuthAccessToken()
	{
		return TWITTER_OAUTH_ACCESS_TOKEN.getName();
	}

	/**
	 * @return A Twitter OAuth Access Token Secret (You have to set this in Constants)
	 */
	public static String getTwitterOAuthAccessTokenSecret()
	{
		return TWITTER_OAUTH_ACESS_TOKEN_SECRET.getName();
	}

}
