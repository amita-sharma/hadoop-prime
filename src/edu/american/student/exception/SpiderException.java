package edu.american.student.exception;

public class SpiderException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7879303142451136027L;

	public SpiderException(String message)
	{
		super(message);
	}

	public SpiderException(String message, Throwable throwable)
	{
		super(message, throwable);
	}
}
