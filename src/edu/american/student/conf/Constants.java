package edu.american.student.conf;

public enum Constants
{
	DEFAULT_TABLE("MAIN"),
	DEFAULT_AUTHS("public"),
	DEFAULT_ACCUMULO_USER("root"),
	DEFAULT_ACCUMULO_PASSWORD("pass"),
	DEFAULT_ACCUMULO_INSTANCE("mnemosyne"),
	
	DEFAULT_ZOOKEEPER_INSTANCE_NAME("mnemosyne"),
	DEFAULT_ZOOKEEPER_INSTANCE("127.0.0.1"), 
	
	TWITTER_OAUTH_CONSUMER_KEY("Ax0OLrkZeTv55jL4JMaTcg"),
	TWITTER_OAUTH_CONSUMER_SECRET("Myzm9mLt4hrpchUbHXBtEJZytan2oOgTUTJrPYmqBo"),
	TWITTER_OAUTH_ACCESS_TOKEN("588434671-7iAor9RjYnfKREv06HRL6xCRkN6TiDQXvabjuxP1"), 
	TWITTER_OAUTH_ACESS_TOKEN_SECRET("1NEHHADQ8p6rj3N0aTpv9ols333v4uSEAtviiZESSuQ");

	private String name = "";

	Constants(String name)
	{
		this.name = name;
	}

	public String getName()
	{
		return this.name;
	}

	public static String getDefaultTable()
	{
		return DEFAULT_TABLE.getName();
	}

	public static String getDefaultAuths()
	{
		return DEFAULT_AUTHS.getName();
	}

	public static String getAccumuloUser()
	{
		return DEFAULT_ACCUMULO_USER.getName();
	}

	public static String getAccumuloPassword()
	{
		return DEFAULT_ACCUMULO_PASSWORD.getName();
	}

	public static String getZookeeperInstanceName()
	{
		return DEFAULT_ZOOKEEPER_INSTANCE_NAME.getName();
	}

	public static String getZookeeperInstance()
	{
		return DEFAULT_ZOOKEEPER_INSTANCE.getName();
	}

	public static String getAccumuloInstance()
	{
		return DEFAULT_ACCUMULO_INSTANCE.getName();
	}

	public static String getTwitterOAuthConsumerKey()
	{
		return TWITTER_OAUTH_CONSUMER_KEY.getName();
	}

	public static String getTwitterOAuthConsumerSecret()
	{
		return TWITTER_OAUTH_CONSUMER_SECRET.getName();
	}

	public static String getTwitterOAuthAccessToken()
	{
		return TWITTER_OAUTH_ACCESS_TOKEN.getName();
	}

	public static String getTwitterOAuthAccessTokenSecret()
	{
		return TWITTER_OAUTH_ACESS_TOKEN_SECRET.getName();
	}

}
