package com.tango.citrine.jobrunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by per on 10/03/15.
 */
public class ThreadPoolExecutorJobRunner implements JobRunner {

    private final Logger logger = LoggerFactory.getLogger(ThreadPoolExecutorJobRunner.class);

    private final ThreadPoolExecutor executor;

    public ThreadPoolExecutorJobRunner(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void initialize() {
        // do nothing
    }

    @Override
    public boolean run(Runnable runnable) {
        try {
            executor.execute(runnable);
            return true;
        } catch (RejectedExecutionException ex) {
            return false;
        }
    }

    @Override
    public int maxRunnablesAccepted() {
        return executor.getMaximumPoolSize() - executor.getActiveCount();
    }

    @Override
    public void shutdown(boolean waitForJobsToComplete) {
        executor.shutdown();
        if(waitForJobsToComplete) {
            boolean interrupted = false;
            try {
                while(!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    logger.warn("Waiting for executor to terminate...");
                }
            } catch (InterruptedException e) {
               interrupted = true;
            } finally {
                if(interrupted) {
                    Thread.interrupted();
                }
            }
        }
    }
}
