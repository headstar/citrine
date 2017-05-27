package com.tango.citrine.jobrunner;

/**
 * A JobRunner is responsible for executing a job. Normally backed by a thread pool.
 */
public interface JobRunner {

    /**
     * Called before the JobRunner is used.
     */
    void initialize();

    /**
     * Executes the given <code>{@link java.lang.Runnable}</code>.
     *
     * @param runnable
     * @return <code>true</code>  if the runnable was accepted for execution.
     */
    boolean run(Runnable runnable);

    /**
     * Returns the maximum number of runnables which will be accepted by {@link #run(Runnable)} before it returns <code>false</code>.
     *
     * E.g. if {@link #maxRunnablesAccepted()} returns 3, {@link #run(Runnable)} SHOULD return <code>true</code> 3 times.
     *
     * @return
     */
    int maxRunnablesAccepted();

    /**
     * Called by the scheduler to inform the <code>JobRunnner</code>
     * that it should free up all of it's resources because the scheduler is
     * shutting down.
     */
    void shutdown(boolean waitForJobsToComplete);

}
