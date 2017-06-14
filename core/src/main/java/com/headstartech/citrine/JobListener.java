package com.headstartech.citrine;

/**
 * The interface to be implemented by classes that want to be informed when a job executes.
 */
public interface JobListener {

    /**
     * Called before {@link Job#execute(JobExecutionContext)} is called.
     *
     * @param context
     */
    void jobToBeExecuted(JobExecutionContext context);

}
