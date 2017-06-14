package com.headstartech.citrine;

/**
 * The interface to be implemented by classes which represent a 'job' to be executed.
 */
public interface Job {

    /**
     * Called by <code>JobScheduler</code> when a job's trigger has fired.
     *
     * @param jobExecutionContext the execution jobExecutionContext available to the job.
     */
    void execute(JobExecutionContext jobExecutionContext);
}
