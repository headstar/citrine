package com.headstartech.citrine.core;


import com.headstartech.citrine.*;
import com.headstartech.citrine.jobrunner.JobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.testutil.TestJobClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by per on 26/03/15.
 */
public class SchedulerImplTest {

    private final static int IDLE_WAIT_TIME = 1000;
    private final static int MAX_BATCH_SIZE  = 100;
    private final MutableSchedulerConfiguration configuration = new MutableSchedulerConfiguration(IDLE_WAIT_TIME, MAX_BATCH_SIZE);

    @Test
    public void initialize() {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        // when
        scheduler.initialize();

        // then
        verify(jobRunnerMock).initialize();
        verify(jobStoreMock).initialize();
    }

    @Test(expected = SchedulerException.class)
    public void startClosedScheduler() throws SchedulerException {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());
        scheduler.start();
        scheduler.shutdown();

        // when
        scheduler.start();

        // then  ...exception should be thrown
    }

    @Test
    public void scheduleJob() throws SchedulerException, InterruptedException {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobDetail jobDetail = createJobDetail();

        // when
        scheduler.scheduleJob(jobDetail, true);

        // then
        verify(jobStoreMock).addJob(jobDetail, true);
    }

    @Test
    public void scheduleJobs() throws SchedulerException, InterruptedException {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobDetail jobDetail1 = createJobDetail();
        JobDetail jobDetail2 = createJobDetail();
        List<JobDetail> jobDetails = new ArrayList<JobDetail>();
        jobDetails.add(jobDetail1);
        jobDetails.add(jobDetail2);

        // when
        scheduler.scheduleJobs(jobDetails);

        // then
        verify(jobStoreMock).addJobs(jobDetails);
    }

    @Test
    public void removeExistingJob() throws SchedulerException, InterruptedException {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobKey jk = new JobKey("job4711");
        when(jobStoreMock.removeJob(jk)).thenReturn(true);

        // when
        boolean res = scheduler.removeJob(jk);

        // then
        assertTrue(res);
        verify(jobStoreMock).removeJob(jk);
    }

    @Test
    public void removeNonExistingJob() throws SchedulerException, InterruptedException {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobKey jk = new JobKey("job4711");
        when(jobStoreMock.removeJob(jk)).thenReturn(false);

        // when
        boolean res = scheduler.removeJob(jk);

        // then
        assertFalse(res);
        verify(jobStoreMock).removeJob(jk);
    }

    @Test(expected = SchedulerException.class)
    public void scheduleJobThrowingRTE() {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobDetail jobDetail = createJobDetail();
        when(jobStoreMock.addJob(jobDetail, true)).thenThrow(new RuntimeException());

        // when
        scheduler.scheduleJob(jobDetail, true);

    }

    @Test(expected = SchedulerException.class)
    public void removeJobThrowingRTE() {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobKey jk = new JobKey("job4711");
        when(jobStoreMock.removeJob(jk)).thenThrow(new RuntimeException());

        // when
        scheduler.removeJob(jk);
    }

    @Test(expected = SchedulerException.class)
    public void existsThrowingRTE() {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobKey jk = new JobKey("job4711");
        when(jobStoreMock.exists(jk)).thenThrow(new RuntimeException());

        // when
        scheduler.exists(jk);
    }

    @Test(expected = SchedulerException.class)
    public void getJobThrowingRTE() {
        // given
        JobRunner jobRunnerMock = mock(JobRunner.class);
        JobStore jobStoreMock = mock(JobStore.class);
        Scheduler scheduler = new SchedulerImpl("aName", configuration, jobStoreMock, jobRunnerMock, new SchedulerContext());

        JobKey jk = new JobKey("job4711");
        when(jobStoreMock.getJob(jk)).thenThrow(new RuntimeException());

        // when
        scheduler.getJob(jk);
    }

    private static JobDetail createJobDetail() {
        JobKey jk = new JobKey(UUID.randomUUID().toString());
        return new JobDetail(jk, TestJobClass.class, new SimpleTrigger(new Date()), new JobData());
    }

}
