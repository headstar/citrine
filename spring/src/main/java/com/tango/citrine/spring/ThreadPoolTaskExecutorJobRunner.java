package com.tango.citrine.spring;

import com.google.common.base.Preconditions;
import com.tango.citrine.jobrunner.JobRunner;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Created by per on 10/03/15.
 */
public class ThreadPoolTaskExecutorJobRunner implements JobRunner {

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    private int minAcceptedThreshold;

    @Override
    public void initialize() {
        threadPoolTaskExecutor = SchedulerFactoryBean.getConfigurationTimeTaskExecutorHolder();
        if(threadPoolTaskExecutor == null) {
            throw new RuntimeException("No taskExecutor found for configuration - " +
                            "'taskExecutor' property must be set on SchedulerFactoryBean");
        }
    }

    @Override
    public boolean run(Runnable runnable) {
        try {
            threadPoolTaskExecutor.execute(runnable);
            return true;
        } catch (TaskRejectedException ex) {
            return false;
        }
    }

    @Override
    public int maxRunnablesAccepted() {
        int num = getMaxRunnablesAccepted();
        return (num < minAcceptedThreshold) ? 0 : num;
    }

    @Override
    public void shutdown(boolean waitForJobsToComplete) {
        // assumes the executor lifecycle is managed by Spring
    }

    protected int getMaxRunnablesAccepted() {
        return numToQueue() + numFreeThreads();
    }

    public int getMinAcceptedThreshold() {
        return minAcceptedThreshold;
    }

    public void setMinAcceptedThreshold(int minAcceptedThreshold) {
        this.minAcceptedThreshold = minAcceptedThreshold;
    }

    private int numFreeThreads() {
        return threadPoolTaskExecutor.getMaxPoolSize() - threadPoolTaskExecutor.getActiveCount();
    }

    private int numToQueue() {
        return threadPoolTaskExecutor.getThreadPoolExecutor().getQueue().remainingCapacity();
    }
}
