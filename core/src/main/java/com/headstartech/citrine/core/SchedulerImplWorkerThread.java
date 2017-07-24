package com.headstartech.citrine.core;


import com.headstartech.citrine.JobDetail;
import com.headstartech.citrine.Scheduler;
import com.headstartech.citrine.SchedulerException;
import com.headstartech.citrine.jobrunner.JobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.TriggeredJob;
import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by per on 3/7/15.
 */
class SchedulerImplWorkerThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(SchedulerImplWorkerThread.class);

    private final JobStore jobStore;
    private final JobRunner jobRunner;
    private final Scheduler scheduler;

    private final Object sigLock = new Object();
    private final AtomicBoolean halted;
    private boolean paused;
    private final SchedulerConfiguration schedulerConfiguration;
    private final Random random = new Random();

    public SchedulerImplWorkerThread(SchedulerConfiguration schedulerConfiguration, JobStore jobStore, JobRunner jobRunner, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.jobRunner = jobRunner;
        this.jobStore = jobStore;
        halted = new AtomicBoolean(false);
        this.schedulerConfiguration = schedulerConfiguration;

        // start in 'paused' state
        this.paused = true;
    }

    @Override
    public void run() {
        while (!halted.get()) {
            try {
                // wait if we're paused
                synchronized (sigLock) {
                    while (paused && !halted.get()) {
                        try {
                            sigLock.wait(1000L);
                        } catch (InterruptedException ignore) {
                        }
                    }

                    if (halted.get()) {
                        break;
                    }
                }

                int maxRunnablesAccepted = jobRunner.maxRunnablesAccepted();
                if (maxRunnablesAccepted > 0) {
                    int batchSize = Math.min(maxRunnablesAccepted, schedulerConfiguration.getMaxBatchSize());
                    Collection<TriggeredJob> jobs = null;
                    try {
                        jobs = jobStore.acquireTriggeredJobs(new Date(), batchSize);
                        logger.debug("Batch acquisition of jobs: count={}", jobs.size());
                    } catch (SchedulerException e) {
                        logger.warn("Failed to acquire jobs", e);
                    }

                    if (jobs != null && !jobs.isEmpty()) {
                        for (TriggeredJob tj : jobs) {
                            JobDetail jd = tj.getJobDetail();
                            try {
                                JobRunShell shell;
                                try {
                                    shell = new JobRunShell(tj, jobStore, scheduler);
                                } catch (SchedulerException e) {
                                    logger.error(String.format("Failed to create run shell for job: jobKey=%s", jd.getJobKey()), e);
                                    jobStore.triggeredJobComplete(tj, TriggeredJobCompleteAction.SET_JOB_ERROR);
                                    continue;
                                }

                                if (!jobRunner.run(shell)) {
                                    logger.warn("Failed to add job to thread pool, retrying job later: jobKey={}", jd.getJobKey());
                                    jobStore.triggeredJobComplete(tj, TriggeredJobCompleteAction.SET_JOB_WAITING);
                                }
                            } catch (RuntimeException e) {
                                logger.error(String.format("Exception when adding job to thread pool: jobKey=%s ", jd.getJobKey()), e);
                                jobStore.triggeredJobComplete(tj, TriggeredJobCompleteAction.SET_JOB_ERROR);
                            }
                        }

                        if (jobs.size() == batchSize) {
                            continue;
                        }
                    }
                }
            } catch (RuntimeException e) {
                logger.error("Runtime exception in main loop: ", e);
            }

            try {
                long waitTime = getRandomizedIdleWaitTime();
                logger.debug("Idling... : waitTime={}", waitTime);
                synchronized (sigLock) {
                    try {
                        if (!halted.get()) {
                            sigLock.wait(waitTime);
                        }
                    } catch (InterruptedException ignore) {
                    }
                }
            } catch(RuntimeException e) {
                logger.warn("Runtime exception when idling: ", e);
            }
        }

        logger.debug("Scheduler worker thread exiting...");
    }

    public void shutdown(boolean wait) {
        synchronized (sigLock) {
            halted.set(true);
            if(paused) {
                sigLock.notifyAll();
            }
        }

        if (wait) {
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        join();
                        break;
                    } catch (InterruptedException ignore) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public boolean isPaused() {
        return paused;
    }

    public void togglePause(boolean pause) {
        synchronized (sigLock) {
            paused = pause;

            if (!paused) {
                sigLock.notifyAll();
            }
        }
    }

    protected long getRandomizedIdleWaitTime() {
        int idleWaitVariableness = (int) (schedulerConfiguration.getIdleWaitTime() * 0.2);
        if(idleWaitVariableness > 0) {
            return schedulerConfiguration.getIdleWaitTime() - random.nextInt(idleWaitVariableness);
        } else {
            return schedulerConfiguration.getIdleWaitTime();
        }
    }

}
