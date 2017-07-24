package com.headstartech.citrine.core;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.headstartech.citrine.*;
import com.headstartech.citrine.jobrunner.JobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by per on 3/7/15.
 */

public class SchedulerImpl implements Scheduler {

    private final Logger logger = LoggerFactory.getLogger(SchedulerImpl.class);

    private enum SchedulerState { CREATED, SHUTTING_DOWN, CLOSED };

    private final SchedulerImplWorkerThread schedulerImplWorkerThread;
    private final JobStore jobStore;
    private final JobRunner jobRunner;
    private final SchedulerContext schedulerContext;
    private final String schedulerName;
    private final ListenerRegistry listenerRegistry;
    private volatile SchedulerState schedulerState;

    public SchedulerImpl(String schedulerName, SchedulerConfiguration configuration, JobStore jobStore, JobRunner jobRunner, SchedulerContext schedulerContext) {
        Preconditions.checkNotNull(schedulerName, "schedulerName cannot be null");
        Preconditions.checkArgument(schedulerName.length() > 0, "schedulerName cannot be blank");
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(jobStore);
        Preconditions.checkNotNull(jobRunner);
        Preconditions.checkNotNull(schedulerContext);
        this.jobStore = jobStore;
        this.jobRunner = jobRunner;
        this.schedulerContext = schedulerContext;
        this.schedulerName = schedulerName;
        this.schedulerState = SchedulerState.CREATED;
        this.listenerRegistry = new ListenerRegistryImpl();

        this.schedulerImplWorkerThread = new SchedulerImplWorkerThread(configuration, jobStore, jobRunner, this);
        this.schedulerImplWorkerThread.setName(String.format("%s-worker-thread", this.schedulerName));
        this.schedulerImplWorkerThread.setDaemon(true);
        this.schedulerImplWorkerThread.start();
    }

    @Override
    public void initialize() {
        jobStore.initialize();
        jobRunner.initialize();
    }

    @Override
    public boolean scheduleJob(JobDetail jobDetail, boolean replaceExisting) {
        logger.debug("Scheduling job: jobDetails={}, replace={}", jobDetail, replaceExisting);
        try {
            return jobStore.addJob(jobDetail, replaceExisting);
        } catch(RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, SchedulerException.class);
            throw new SchedulerException(e);
        }
    }

    @Override
    public void scheduleJobs(Iterable<JobDetail> jobDetails) {
        try {
            jobStore.addJobs(jobDetails);
        } catch(RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, SchedulerException.class);
            throw new SchedulerException(e);
        }
    }

    @Override
    public boolean removeJob(JobKey jobKey) {
        logger.debug("Removing job: jobKey={}", jobKey);
        try {
            return jobStore.removeJob(jobKey);
        } catch(RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, SchedulerException.class);
            throw new SchedulerException(e);
        }
    }

    @Override
    public boolean exists(JobKey jobKey) {
        try {
            return jobStore.exists(jobKey);
        } catch(RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, SchedulerException.class);
            throw new SchedulerException(e);
        }
    }

    @Override
    public Optional<JobDetail> getJob(JobKey jobKey) {
        try {
            return jobStore.getJob(jobKey);
        } catch(RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, SchedulerException.class);
            throw new SchedulerException(e);
        }
    }

    @Override
    public SchedulerContext getContext() {
        return schedulerContext;
    }

    @Override
    public synchronized void start() {
        if(SchedulerState.SHUTTING_DOWN.equals(schedulerState) || SchedulerState.CLOSED.equals(schedulerState)) {
            throw new SchedulerException("The scheduler cannot be started after shutdown() has been called.");
        }

        schedulerImplWorkerThread.togglePause(false);
        logger.info("Scheduler started: schedulerName={}", schedulerName);
    }

    @Override
    public void pause() {
        schedulerImplWorkerThread.togglePause(true);
        logger.info("Scheduler paused: schedulerName={}", schedulerName);

    }

    @Override
    public boolean isPaused() {
        return schedulerImplWorkerThread.isPaused();
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    @Override
    public synchronized void shutdown(boolean waitForJobsToComplete) {
        schedulerState = SchedulerState.SHUTTING_DOWN;
        logger.info("Shutting down scheduler: schedulerName={}, waitForJobsToComplete={}", schedulerName, waitForJobsToComplete);
        try {
            schedulerImplWorkerThread.shutdown(true);
        } catch(RuntimeException e) {
            logger.warn("Worker thread threw unhandled exception when shutting down", e);
        }
        try {
            jobRunner.shutdown(waitForJobsToComplete);
        } catch(RuntimeException e) {
            logger.warn("Job runner threw unhandled exception when shutting down", e);
        }
        try {
            jobStore.shutdown();
        } catch(RuntimeException e) {
            logger.warn("Job store threw unhandled exception when shutting down", e);
        }
        schedulerState = SchedulerState.CLOSED;
        logger.info("Scheduler shut down: schedulerName={}", schedulerName);
    }

    @Override
    public ListenerRegistry getListenerRegistry() {
        return listenerRegistry;
    }
}
