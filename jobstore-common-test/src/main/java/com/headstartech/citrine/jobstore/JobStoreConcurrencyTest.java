package com.headstartech.citrine.jobstore;

import com.google.common.collect.Sets;
import com.headstartech.citrine.*;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by per on 30/03/15.
 */
public abstract class JobStoreConcurrencyTest extends JobStoreCommonTestBase {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(JobStoreConcurrencyTest.class);

    /**
     * This a 'smoke test' checking nothing bad happens when multiple threads try to acquire jobs concurrently.
     *
     * @throws JobAlreadyExistsException
     * @throws InterruptedException
     */
    @Test
    public void acquireJobsConcurrently() throws JobAlreadyExistsException, InterruptedException {
        Set<JobDetail> jobDetails = new HashSet<JobDetail>();

        try {
            // given
            final int numJobs = 200;
            for (int i = 0; i < numJobs; ++i) {
                JobKey jk = new JobKey(String.format("job-%d", i));
                JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date(1234)), new JobData());
                jobDetails.add(jd);
                jobStore.addJob(jd, true);
            }

            final int numThreads = 10;
            final int maxCount = 20;
            List<Thread> threads = new ArrayList<Thread>();
            List<JobStoreAcquireRunnable> acquireRunnables = new ArrayList<JobStoreAcquireRunnable>();
            for(int i=0; i<numThreads; i++) {
                JobStoreAcquireRunnable runnable = new JobStoreAcquireRunnable(jobStore, maxCount);
                acquireRunnables.add(runnable);
                Thread t = new Thread(runnable);
                t.setName(String.format("jobstore-%d", i));
                threads.add(t);
            }

            // when
            for(Thread t : threads) {
                t.start();
            }
            for(Thread t : threads) {
                t.join();
            }

            // then
            Set<JobDetail> allAcquiredJobs = new HashSet<JobDetail>();
            for(JobStoreAcquireRunnable runnable : acquireRunnables) {
                assertNull(runnable.exceptionThrown());
                Set<JobDetail> jobsAcquiredByThisRunnable = new HashSet<JobDetail>();
                for(TriggeredJob tj : runnable.getAcquiredJobs()) {
                    jobsAcquiredByThisRunnable.add(tj.getJobDetail());
                }

                assertEquals(0, Sets.intersection(allAcquiredJobs, jobsAcquiredByThisRunnable).size());  // check jobs haven't been acquired by someone else
                allAcquiredJobs.addAll(jobsAcquiredByThisRunnable);
            }

            assertEquals(allAcquiredJobs.size(), numJobs);
        } finally {
            for(JobDetail jd : jobDetails) {
                jobStore.removeJob(jd.getJobKey());
            }
        }

    }

    private static class JobStoreAcquireRunnable implements Runnable {

        private final JobStore jobStore;
        private final int maxCount;
        private Collection<TriggeredJob> acquiredJobs = new ArrayList<TriggeredJob>();
        private Exception exceptionThrown = null;

        public JobStoreAcquireRunnable(JobStore jobStore, int maxCount) {
            this.jobStore = jobStore;
            this.maxCount = maxCount;
        }

        @Override
        public void run() {
            try {
                logger.info("Running {}", Thread.currentThread().getName());
                acquiredJobs = jobStore.acquireTriggeredJobs(new Date(), maxCount);
                logger.info("Acquired jobs {}", acquiredJobs.size());
            } catch (Exception e) {
                exceptionThrown = e;
            }
        }

        public Collection<TriggeredJob> getAcquiredJobs() {
            return acquiredJobs;
        }

        public Exception exceptionThrown() {
            return exceptionThrown;
        }
    }
}
