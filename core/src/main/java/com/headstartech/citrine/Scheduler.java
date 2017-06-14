package com.headstartech.citrine;

import com.google.common.base.Optional;

/**
 * Main interface of the scheduler. The scheduler keeps track of scheduled jobs and executes them when their fire time has arrived.
 */
public interface Scheduler {

    /**
     * Initializes the scheduler. SHOULD be called after instance has been created!
     */
    void initialize();

    /**
     * Schedules a job for execution.
     *
     * Note, it's possible to schedule jobs before {@link #start()} has been called.
     *
     * @param jobDetail <code>JobDetail</code> object containing the job configuration.
     * @param replaceExisting if <code>true</code>, an existing job (with the same <code>JobKey</code>) will be replaced.
     * @return <code>true</code> if the job existed, <code>false</code> otherwise.
     * @throws SchedulerException if the job cannot't be added or there is an internal scheduler error.
     */
    boolean scheduleJob(JobDetail jobDetail, boolean replaceExisting);

    /**
     * Schedules jobs for execution.
     *
     *
     * @param jobDetails
     */
    void scheduleJobs(Iterable<JobDetail> jobDetails);

    /**
     * Removes the job from the scheduler.
     *
     * Note, it's possible to remove jobs before {@link #start()} has been called.
     *
     * @param jobKey
     * @return <code>true</code> if the job existed, <code>false</code> otherwise.
     * @throws SchedulerException if the job cannot be removed or there is an internal scheduler error.
     */
    boolean removeJob(JobKey jobKey);

    /**
     * Checks if a job exists.
     *
     * @param jobKey
     * @return <code>true</code> if the job exists, <code>false</code> otherwise.
     * @throws SchedulerException if there is an internal scheduler error.
     */
    boolean exists(JobKey jobKey);

    /**
     * Returns a <code>JobDetail</code> representing the specified job.
     *
     * @param jobKey
     * @return the job detail in a com.google.common.base.Optional if it exists, Optional.absent() otherwise.
     * @throws SchedulerException if there is an internal scheduler error.
     */
    Optional<JobDetail> getJob(JobKey jobKey);

    /**
     * Returns the <code>JobSchedulerContext</code>.
     *
     * @return
     */
    SchedulerContext getContext();

    /**
     * Starts the scheduler's thread executing jobs. When the scheduler is first created, no job trigger will fire until this method has been called (but it is
     * possible to schedule and remove jobs before start() has been called).
     *
     * The scheduler cannot be re-started.
     *
     * @throws SchedulerException if the scheduler cannot be started.
     */
    void start();

    /**
     * Stops the scheduler's firing of job triggers. Equivalent to <code>shutdown(false)</code>.
     *
     */
    void shutdown();

    /**
     * Stops the scheduler's firing of job triggers.
     *
     * The scheduler cannot be re-started.
     *
     * @param waitForJobsToComplete  if <code>true</code> the method will not return until all currently executing jobs have completed.
     */
    void shutdown(boolean waitForJobsToComplete);

    /**
     * Gets the <code>ListenerRegistry</code>.
     * @return
     */
    ListenerRegistry getListenerRegistry();

}
