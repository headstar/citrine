package com.headstartech.citrine.testutil;

import com.google.common.base.Optional;
import com.headstartech.citrine.*;
import com.headstartech.citrine.core.ListenerRegistryImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by per on 27/03/15.
 */
public class TestUtil {

    private TestUtil() {}

    public static String JOBS_EXECUTED_KEY = "JobsExecuted";

    public static JobsExecuted getJobsExecuted(Scheduler scheduler) {
        return (TestUtil.JobsExecuted) scheduler.getContext().get(TestUtil.JOBS_EXECUTED_KEY);
    }

    public static Scheduler createJobSchedulerStub() {
       return new SchedulerStub();
    }

    private static class SchedulerStub implements Scheduler {

        private final SchedulerContext ctx;
        private final ListenerRegistry listenerRegistry;

        SchedulerStub() {
            listenerRegistry = new ListenerRegistryImpl();
            ctx = new SchedulerContext();
            ctx.put(JOBS_EXECUTED_KEY, new JobsExecuted());
        }

        @Override
        public void initialize() {

        }

        @Override
        public boolean scheduleJob(JobDetail jobDetail, boolean replaceExisting) throws JobAlreadyExistsException {
            return false;
        }

        @Override
        public void scheduleJobs(Iterable<JobDetail> jobDetails) {

        }

        @Override
        public boolean removeJob(JobKey jobKey) {
            return false;
        }

        @Override
        public boolean exists(JobKey jobKey) {
            return false;
        }

        @Override
        public Optional<JobDetail> getJob(JobKey jobKey) throws SchedulerException {
            return Optional.absent();
        }

        @Override
        public SchedulerContext getContext() {
            return ctx;
        }

        @Override
        public void start() {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void shutdown(boolean waitForJobsToComplete) {

        }

        @Override
        public void pause() {

        }

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public ListenerRegistry getListenerRegistry() {
            return listenerRegistry;
        }

    }

    public static class JobsExecuted {

        private Map<JobKey, JobDetail> jobsExecuted = new HashMap<JobKey, JobDetail>();
        private Map<JobKey, Integer> executedCount = new HashMap<JobKey, Integer>();
        int totalNumberOfExecutions = 0;
        CountDownLatch countDownLatch;

        public void setExpectedNumberOfJobsToBeExecuted(int count) {
            countDownLatch = new CountDownLatch(count);
        }

        public synchronized void addJob(JobDetail jobDetail) {
            JobKey jk = jobDetail.getJobKey();
            jobsExecuted.put(jk, jobDetail);
            Integer count = executedCount.get(jk);
            if(count == null) {
                count = 0;
            }
            count++;
            executedCount.put(jk, count);
            totalNumberOfExecutions++;
            if(countDownLatch != null) {
                countDownLatch.countDown();
            }
        }

        public synchronized int countJobsExecuted() {
            return jobsExecuted.size();
        }

        public synchronized boolean contains(JobKey jobKey) {
            return jobsExecuted.containsKey(jobKey);
        }

        public synchronized int countNumberOfTimesJobWasExecuted(JobKey jobKey) {
            Integer count = executedCount.get(jobKey);
            return count == null ? 0 : count;
        }

        public synchronized  int countTotalNumberOfExecutions() {
            return totalNumberOfExecutions;
        }

        public void waitForAllJobsToBeExecuted(long timeout, TimeUnit unit) throws InterruptedException {
            countDownLatch.await(timeout, unit);
        }
    }
}
