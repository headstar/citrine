package com.tango.citrine.core;


import com.tango.citrine.*;
import com.tango.citrine.jobstore.JobStore;
import com.tango.citrine.jobstore.TriggeredJob;
import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;
import com.tango.citrine.testutil.TestJobClass;
import com.tango.citrine.testutil.TestUtil;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by per on 27/03/15.
 */
public class JobRunShellTest {

    private final Date scheduledFireTime = new Date(1000);
    private final Date actualFireTime = new Date(2000);

    @Test
    public void runJob() throws SchedulerException {
        // given
        JobStore jobStoreMock = mock(JobStore.class);
        JobListener jobListenerMock = mock(JobListener.class);

        JobKey jk = new JobKey("job4711");
        JobDetail jd = new JobDetail(jk, TestJobClass.class, new SimpleTrigger(new Date()), new JobData());

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, scheduledFireTime, actualFireTime);
        Scheduler scheduler = TestUtil.createJobSchedulerStub();
        scheduler.getListenerRegistry().addJobListener(jobListenerMock);
        JobRunShell jobRunShell = new JobRunShell(triggeredJob, jobStoreMock, scheduler);

        // when
        jobRunShell.run();

        // then
        TestUtil.JobsExecuted jobsExecuted = TestUtil.getJobsExecuted(scheduler);
        assertEquals(1, jobsExecuted.countJobsExecuted());
        assertTrue(jobsExecuted.contains(jk));
        verify(jobListenerMock).jobToBeExecuted(any(JobExecutionContext.class));
        verify(jobStoreMock).triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
   }

    @Test
    public void RunJobExceptionThrownWhenExecutingJob() throws SchedulerException {
        // given
        JobStore jobStoreMock = mock(JobStore.class);
        JobKey jk = new JobKey("job4711");
        JobDetail jd = new JobDetail(jk, ExceptionThrowingJobClass.class, new SimpleTrigger(new Date()), new JobData());

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, scheduledFireTime, actualFireTime);
        Scheduler scheduler = TestUtil.createJobSchedulerStub();
        JobRunShell jobRunShell = new JobRunShell(triggeredJob, jobStoreMock, scheduler);

        // when
        jobRunShell.run();

        // then
        verify(jobStoreMock).triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
    }


    @Test(expected = SchedulerException.class)
    public void RunJobWithInaccessibleJobClass() throws SchedulerException {
        // given
        JobStore jobStoreMock = mock(JobStore.class);
        JobKey jk = new JobKey("job4711");
        JobDetail jd = new JobDetail(jk, InaccessibleJobClass.class, new SimpleTrigger(new Date()), new JobData());

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, scheduledFireTime, actualFireTime);
        Scheduler scheduler = TestUtil.createJobSchedulerStub();

        // when
        new JobRunShell(triggeredJob, jobStoreMock, scheduler);

        // then ...exception should be thrown

    }

    public static class ExceptionThrowingJobClass implements Job {
        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            throw new RuntimeException();
        }
    }

    // 'private' makes class not accessible for job run shell
    private static class InaccessibleJobClass implements Job {

        @Override
        public void execute(JobExecutionContext jobExecutionContext) {

        }
    }

}
