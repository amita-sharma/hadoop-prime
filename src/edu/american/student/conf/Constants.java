package edu.american.student.conf;

public enum Constants
{
	DEFAULT_TABLE("MAIN"),
	DEFAULT_AUTHS("public"),
	DEFAULT_ACCUMULO_USER("root"),
	DEFAULT_ACCUMULO_PASSWORD("pass"),
	DEFAULT_ZOOKEEPER_INSTANCE_NAME("zkInstance"),
	DEFAULT_ZOOKEEPER_INSTANCE("localhost"),
	DEFAULT_ACCUMULO_INSTANCE("main");

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

}
