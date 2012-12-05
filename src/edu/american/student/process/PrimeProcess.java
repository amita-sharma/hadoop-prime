package edu.american.student.process;

import edu.american.student.conf.ProcessConfiguration;
import edu.american.student.exception.ProcessException;

public interface PrimeProcess
{

	public void initalize(ProcessConfiguration conf) throws ProcessException;
	public void start() throws ProcessException;
}
