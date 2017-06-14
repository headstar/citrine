package com.headstartech.citrine.core;

import com.google.common.collect.Lists;
import com.headstartech.citrine.*;
import com.headstartech.citrine.jobrunner.JobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.TriggeredJob;
import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import com.headstartech.citrine.jobstore.VersionedTriggeredJob;
import com.headstartech.citrine.testutil.TestJobClass;
import com.headstartech.citrine.testutil.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Created by per on 27/03/15.
 */
public class SchedulerImpWorkerThreadTest {

    private final Logger logger = LoggerFactory.getLogger(SchedulerImpWorkerThreadTest.class);

    private static final int IDLE_WAIT_TIME = 20000;

    private static final SchedulerConfiguration schedulerConf = new MutableSchedulerConfiguration(IDLE_WAIT_TIME, 1000);

    @Test
    public void basicFlow() throws InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);
            TriggeredJob triggeredJob = createTriggeredJob();

            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            TestUtil.JobsExecuted jobsExecuted = TestUtil.getJobsExecuted(scheduler);
            jobsExecuted.setExpectedNumberOfJobsToBeExecuted(1);

            workerThread = new SchedulerImplWorkerThread(schedulerConf, jobStoreMock, new SerialJobRunner(), scheduler);

            // when
            workerThread.start();

            // then
            jobsExecuted.waitForAllJobsToBeExecuted(1000, TimeUnit.MILLISECONDS);
            assertEquals(1, jobsExecuted.countJobsExecuted());
            assertEquals(1, jobsExecuted.countTotalNumberOfExecutions());
            assertTrue(jobsExecuted.contains(triggeredJob.getJobDetail().getJobKey()));
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    @Test
    public void jobRunnerNotAcceptingJob() throws InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);
            JobRunner jobRunnerMock = mock(JobRunner.class);
            TriggeredJob triggeredJob = createTriggeredJob();

            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));
            when(jobRunnerMock.maxRunnablesAccepted()).thenReturn(10);  // saying the job runner will accept 10 runnables
            when(jobRunnerMock.run(any(Runnable.class))).thenReturn(false);  // but saying no when actually doing it

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            workerThread = new SchedulerImplWorkerThread(schedulerConf, jobStoreMock, jobRunnerMock, scheduler);

            // when
            workerThread.start();

            // then
            verify(jobStoreMock, timeout(10000)).triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_WAITING);
            TestUtil.JobsExecuted jobsExecuted = TestUtil.getJobsExecuted(scheduler);
            assertEquals(0, jobsExecuted.countTotalNumberOfExecutions());
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    @Test
    public void startAndShutdown() throws InterruptedException {
        // given
        Scheduler scheduler = TestUtil.createJobSchedulerStub();
        JobStore jobStoreMock = mock(JobStore.class);
        SchedulerConfiguration schedulerConf = new MutableSchedulerConfiguration(100, 1000);
        SchedulerImplWorkerThread workerThread = new SchedulerImplWorkerThread(schedulerConf, jobStoreMock, new SerialJobRunner(), scheduler);

        workerThread.start();
        Thread.sleep(1000);  // let thread run for a while

        // when
        workerThread.shutdown(true);

        // then ...no exception should be thrown
    }

    @Test
    public void exceptionWhenCreatingJobRunShell() throws InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);

            JobDetail jd = new JobDetail(new JobKey("job4711"), InaccessibleJobClass.class, new SimpleTrigger(new Date(1000)), new JobData());
            TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());
            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            workerThread = new SchedulerImplWorkerThread(schedulerConf, jobStoreMock, new SerialJobRunner(), scheduler);

            // when
            workerThread.start();

            // then
            verify(jobStoreMock, timeout(10000)).triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_ERROR);
            TestUtil.JobsExecuted jobsExecuted = TestUtil.getJobsExecuted(scheduler);
            assertEquals(0, jobsExecuted.countTotalNumberOfExecutions());
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    @Test
    public void exceptionWhenAddingJobToJobRunner() throws JobAlreadyExistsException, InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);
            JobRunner jobRunnerMock = mock(JobRunner.class);
            TriggeredJob triggeredJob = createTriggeredJob();

            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));
            when(jobRunnerMock.maxRunnablesAccepted()).thenReturn(10);  // saying the job runner will accept 10 runnables
            when(jobRunnerMock.run(any(Runnable.class))).thenThrow(new RuntimeException());  // but throwing exception when actually doing it

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            workerThread = new SchedulerImplWorkerThread(schedulerConf, jobStoreMock, jobRunnerMock, scheduler);

            // when
            workerThread.start();

            // then
            verify(jobStoreMock, timeout(10000)).triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_ERROR);
            TestUtil.JobsExecuted jobsExecuted = TestUtil.getJobsExecuted(scheduler);
            assertEquals(0, jobsExecuted.countTotalNumberOfExecutions());
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    @Test
    public void schedulerMaxBatchSizeEffectiveLimit() throws JobAlreadyExistsException, InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);
            JobRunner jobRunnerMock = mock(JobRunner.class);
            TriggeredJob triggeredJob = createTriggeredJob();


            int schedulerMaxBatchSize = 5;
            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));
            when(jobRunnerMock.maxRunnablesAccepted()).thenReturn(10);
            when(jobRunnerMock.run(any(Runnable.class))).thenReturn(true);

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            workerThread = new SchedulerImplWorkerThread(new MutableSchedulerConfiguration(IDLE_WAIT_TIME, schedulerMaxBatchSize), jobStoreMock, jobRunnerMock, scheduler);

            // when
            workerThread.start();

            // then
            verify(jobStoreMock, timeout(10000)).acquireTriggeredJobs(any(Date.class), eq(schedulerMaxBatchSize));
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    @Test
    public void jobRunnerMaxRunnablesAcceptedEffectiveLimit() throws JobAlreadyExistsException, InterruptedException {
        SchedulerImplWorkerThread workerThread = null;
        try {
            // given
            JobStore jobStoreMock = mock(JobStore.class);
            JobRunner jobRunnerMock = mock(JobRunner.class);
            TriggeredJob triggeredJob = createTriggeredJob();


            int schedulerMaxBatchSize = 10;
            when(jobStoreMock.acquireTriggeredJobs(any(Date.class), anyInt())).thenReturn(Lists.newArrayList(triggeredJob));
            when(jobRunnerMock.maxRunnablesAccepted()).thenReturn(5);
            when(jobRunnerMock.run(any(Runnable.class))).thenReturn(true);

            Scheduler scheduler = TestUtil.createJobSchedulerStub();
            workerThread = new SchedulerImplWorkerThread(new MutableSchedulerConfiguration(IDLE_WAIT_TIME, schedulerMaxBatchSize), jobStoreMock, jobRunnerMock, scheduler);

            // when
            workerThread.start();

            // then
            verify(jobStoreMock, timeout(10000)).acquireTriggeredJobs(any(Date.class), eq(5));
        } finally {
            if(workerThread != null) {
                workerThread.shutdown(false);
            }
        }
    }

    private static class SerialJobRunner implements JobRunner {

        @Override
        public void initialize() {

        }

        @Override
        public boolean run(Runnable runnable) {
            runnable.run();
            return true;
        }

        @Override
        public int maxRunnablesAccepted() {
            return 10;
        }

        @Override
        public void shutdown(boolean waitForJobsToComplete) {

        }
    }

    // 'private' makes class not accessible for job run shell, causes exception when creating job run shell
    private static class InaccessibleJobClass implements Job {

        @Override
        public void execute(JobExecutionContext jobExecutionContext) {

        }
    }

    private static TriggeredJob createTriggeredJob() {
        return new VersionedTriggeredJob(createJobDetail(), 0, new Date(), new Date());
    }

    private static JobDetail createJobDetail() {
        JobKey jk = new JobKey(UUID.randomUUID().toString());
        return new JobDetail(jk, TestJobClass.class, new SimpleTrigger(new Date()), new JobData());
    }
}
