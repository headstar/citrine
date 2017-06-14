package com.headstartech.citrine.jobstore;

import com.headstartech.citrine.*;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * NOTE! Assumes the implementation of {@link JobStore#triggeredJobComplete(TriggeredJob, TriggeredJobCompleteAction)} is handled synchronously.
 */
public abstract class JobCompletionTests extends JobStoreCommonTestBase {

    @Test
    public void triggeredJobWithSimpleTriggerCompleteDeleteJob() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);

        // then
        assertFalse(jobStore.exists(jk));
    }

    @Test
    public void triggeredJobWithSimpleTriggerCompleteDeleteJobWhenChangedConcurrently() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);  // version will be 0
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 17, new Date(), new Date());   // set expected version to be 17 (17 != 0) to fake an update has been made

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);

        // then
        assertTrue(jobStore.exists(jk));  // should still exist!
    }

    @Test
    public void triggeredJobCompleteSetJobError() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_ERROR);

        // then
        assertEquals(JobState.ERROR, jobStore.getJobState(jk).get());
    }

    @Test
    public void triggeredJobCompleteSetJobErrorWhenChangedConcurrently() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);   // version will be 0
        assertEquals(JobState.WAITING, jobStore.getJobState(jk).get());  //

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 17, new Date(), new Date());  // set expected version to be 17 (17 != 0) to fake an update has been made

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_ERROR);

        // then
        assertEquals(JobState.WAITING, jobStore.getJobState(jk).get());  // should still be waiting
    }

    @Test
    public void triggeredJobCompleteSetJobErrorWhenDeleted() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_ERROR);

        // then
        assertFalse(jobStore.exists(jk));
    }

    @Test
    public void triggeredJobCompleteSetJobWaiting() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_WAITING);

        // then
        assertEquals(JobState.WAITING, jobStore.getJobState(jk).get());
    }

    @Test
    public void triggeredJobCompleteSetJobWaitingWhenChangedConcurrently() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);   // version will be 0
        jobStore.triggeredJobComplete(new VersionedTriggeredJob(jd, 0, new Date(), new Date()), TriggeredJobCompleteAction.SET_JOB_ERROR);
        assertEquals(JobState.ERROR, jobStore.getJobState(jk).get());

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 17, new Date(), new Date());  // set expected version to be 17 (17 != 0) to fake an update has been made

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_WAITING);

        // then
        assertEquals(JobState.ERROR, jobStore.getJobState(jk).get());  // should still be ERROR
    }

    @Test
    public void triggeredJobCompleteSetJobWaitingWhenDeleted() throws SchedulerException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.SET_JOB_WAITING);

        // then
        assertFalse(jobStore.exists(jk));
    }

    @Test
    public void triggeredJobWithCronTriggerCompleteDeleteOrRenewJob() throws SchedulerException, InterruptedException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd1 = new JobDetail(jk, TestJobClass1.class, new CronTrigger("0/1 * * * * *"), new JobData());  // every second

        jobStore.addJob(jd1, false);
        Thread.sleep(2000);  // should have passed the firetime for jd1
        List<TriggeredJob> triggeredJobs1 = jobStore.acquireTriggeredJobs(new Date(), Integer.MAX_VALUE);
        TriggeredJob tjJD1 = null;
        for(TriggeredJob tj : triggeredJobs1) {
            if(tj.getJobDetail().equals(jd1)) {
                tjJD1 = tj;
            }
        }
        assertNotNull(tjJD1);
        jobStore.triggeredJobComplete(tjJD1, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
        Thread.sleep(2000);  // should have passed the firetime for jd1

        // when
        List<TriggeredJob> triggeredJobs2 = jobStore.acquireTriggeredJobs(new Date(), Integer.MAX_VALUE);

        // then
        TriggeredJob tjJD2 = null;
        for(TriggeredJob tj : triggeredJobs1) {
            if(tj.getJobDetail().equals(jd1)) {
                tjJD2 = tj;
                break;
            }
        }
        assertNotNull(tjJD2);
    }


    @Test
    public void triggeredJobWithCronTriggerCompleteDeleteOrRescheduleJobWhenDeleted() throws SchedulerException, InterruptedException {
        // given
        JobKey jk = getUniqueJobKey();
        JobDetail jd1 = new JobDetail(jk, TestJobClass1.class, new CronTrigger("0/1 * * * * *"), new JobData());  // every second

        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd1, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);

        // then
        assertFalse(jobStore.exists(jk));
    }
}
