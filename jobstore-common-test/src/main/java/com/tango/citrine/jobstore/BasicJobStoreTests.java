package com.tango.citrine.jobstore;

import com.google.common.base.Optional;
import com.tango.citrine.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by per on 08/12/15.
 */
public abstract class BasicJobStoreTests extends JobStoreCommonTestBase {

    @Test
    public void addJobReplaceFalseJobDoesntExist() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());
        JobData jobData = new JobData();
        jobData.put("a", "b");
        jobData.put("c", "&=/!$%");
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), jobData);
        assertFalse(jobStore.exists(jk));

        // when
        boolean replaced = jobStore.addJob(jd, false);

        // then
        assertFalse(replaced);
        assertTrue(jobStore.exists(jk));
        assertEquals(JobState.WAITING, jobStore.getJobState(jk).get());
    }

    @Test(expected = JobAlreadyExistsException.class)
    public void addJobReplaceFalseJobExists() throws JobAlreadyExistsException {
        // given
        JobKey jk = new JobKey(getUniqueKey());
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData(), (short) 19);
        jobStore.addJob(jd, false);
        assertTrue(jobStore.exists(jk));

        // when
        jobStore.addJob(jd, false);

        // then ... JobAlreadyExistsException should be thrown
    }

    @Test
    public void addJobReplaceTrueJobExists() throws SchedulerException {
        // given
        JobDetail jdOld = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jdOld, false);
        assertTrue(jobStore.exists(jdOld.getJobKey()));

        JobDetail jdNew = new JobDetail(jdOld.getJobKey(), TestJobClass2.class, new SimpleTrigger(new Date(1000)), new JobData(), (short) 19);

        // when
        boolean replaced = jobStore.addJob(jdNew, true);

        // then
        assertTrue(replaced);
        assertTrue(jobStore.exists(jdNew.getJobKey()));
        assertJobDetailEquals(jdNew, jobStore.getJob(jdNew.getJobKey()).get());
        assertEquals(JobState.WAITING, jobStore.getJobState(jdNew.getJobKey()).get());
    }

    @Test
    public void addJobs() throws SchedulerException {
        // given
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        assertFalse(jobStore.exists(jd1.getJobKey()));
        assertFalse(jobStore.exists(jd2.getJobKey()));

        List<JobDetail> jobDetails = new ArrayList<JobDetail>();
        jobDetails.add(jd1);
        jobDetails.add(jd2);

        // when
        jobStore.addJobs(jobDetails);

        // then
        assertTrue(jobStore.exists(jd1.getJobKey()));
        assertEquals(JobState.WAITING, jobStore.getJobState(jd1.getJobKey()).get());
        assertTrue(jobStore.exists(jd2.getJobKey()));
        assertEquals(JobState.WAITING, jobStore.getJobState(jd2.getJobKey()).get());
    }


    @Test(expected = JobAlreadyExistsException.class)
    public void addJobsJobExists() throws SchedulerException {
        // given
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd1, false);
        assertTrue(jobStore.exists(jd1.getJobKey()));
        assertFalse(jobStore.exists(jd2.getJobKey()));

        List<JobDetail> jobDetails = new ArrayList<JobDetail>();
        jobDetails.add(jd1);
        jobDetails.add(jd2);

        // when
        jobStore.addJobs(jobDetails);

        // then ...JobAlreadyExistsException should be thrown
    }

    @Test
    public void addCronTriggerJob() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());;
        JobData jobData = new JobData();
        jobData.put("a", "b");
        jobData.put("c", "&=/!$%");
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new CronTrigger("0 0 * * * *"), jobData);
        assertFalse(jobStore.exists(jk));

        // when
        jobStore.addJob(jd, false);

        // then
        assertTrue(jobStore.exists(jk));
        assertEquals(JobState.WAITING, jobStore.getJobState(jk).get());
    }

    @Test
    public void removeExistingJob() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());;
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        jobStore.addJob(jd, false);
        assertTrue(jobStore.exists(jk));

        // when
        boolean removed = jobStore.removeJob(jk);

        // then
        assertTrue(removed);
        assertFalse(jobStore.exists(jk));
    }

    @Test
    public void removeNonExistingJob() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());;
        assertFalse(jobStore.exists(jk));

        // when
        boolean removed = jobStore.removeJob(jk);

        // then
        assertFalse(removed);
        assertFalse(jobStore.exists(jk));
    }

    @Test
    public void getExistingJob() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());
        JobData jobData = new JobData();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date(10000)), jobData, (short) 17);
        jobStore.addJob(jd, false);

        // when
        Optional<JobDetail> jobDetail = jobStore.getJob(jk);

        // then
        assertTrue(jobDetail.isPresent());
        assertJobDetailEquals(jd, jobDetail.get());
    }

    @Test
    public void getNonExistingJob() throws SchedulerException {
        // given
        JobKey jk = new JobKey(getUniqueKey());

        // when
        Optional<JobDetail> jobDetail = jobStore.getJob(jk);

        // then
        assertFalse(jobDetail.isPresent());
    }

    /**
     * Checks trigger time when acquiring jobs
     * @throws SchedulerException
     */
    @Test
    public void acquireTriggeredJobs() throws SchedulerException {
        // given
        JobData jobData = new JobData();
        jobData.put("a", "b");
        jobData.put("c", "&=/!$%");
        jobData.put("d", null);
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), jobData);
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), jobData);
        JobDetail jd3 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(6000)), jobData);

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);
        jobStore.addJob(jd3, false);

        // when
        List<TriggeredJob> triggeredJobs = jobStore.acquireTriggeredJobs(new Date(5000), 10000);

        // then
        assertTrue(triggeredJobs.size() >= 2);

        for(TriggeredJob triggeredJob : triggeredJobs) {
            assertNotNull(triggeredJob.scheduledExecutionTime());
            assertNotNull(triggeredJob.actualExecutionTime());
        }

        List<JobDetail> jobDetails = collectJobDetails(triggeredJobs);
        assertTrue(jobDetails.contains(jd1));
        assertTrue(jobDetails.contains(jd2));
        assertFalse(jobDetails.contains(jd3));
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd1.getJobKey()).get());
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd2.getJobKey()).get());
        assertEquals(JobState.WAITING, jobStore.getJobState(jd3.getJobKey()).get());
        assertTrue(jobDetails.indexOf(jd2) < jobDetails.indexOf(jd1));
        assertJobDetailEquals(jd1, jobDetails.get(jobDetails.indexOf(jd1)));
        assertJobDetailEquals(jd2, jobDetails.get(jobDetails.indexOf(jd2)));
    }

    @Test
    public void acquireTriggeredJobsMaxCountZero() throws SchedulerException {
        // given
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), new JobData());
        JobDetail jd3 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(2000)), new JobData());

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);
        jobStore.addJob(jd3, false);

        // when
        List<TriggeredJob> triggeredJobs = jobStore.acquireTriggeredJobs(new Date(5000), 0);

        // then
        assertTrue(triggeredJobs.isEmpty());
    }

    @Test
    public void acquireTriggeredJobsMaxCount() throws SchedulerException {
        // given
        List<TriggeredJob> triggeredJobs1 = jobStore.acquireTriggeredJobs(new Date(5000), Integer.MAX_VALUE - 2);
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), new JobData());
        JobDetail jd3 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(2000)), new JobData());

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);
        jobStore.addJob(jd3, false);

        // when
        List<TriggeredJob> triggeredJobs2 = jobStore.acquireTriggeredJobs(new Date(5000), triggeredJobs1.size() + 2);

        // then
        assertEquals(2, triggeredJobs2.size() - triggeredJobs1.size());
        List<JobDetail> jobDetails = collectJobDetails(triggeredJobs2);
        assertFalse(jobDetails.contains(jd1));
        assertTrue(jobDetails.contains(jd2));
        assertTrue(jobDetails.contains(jd3));
        assertEquals(JobState.WAITING, jobStore.getJobState(jd1.getJobKey()).get());
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd2.getJobKey()).get());
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd3.getJobKey()).get());
    }

    @Test
    public void acquireTriggeredJobsWithPriorities() throws SchedulerException {
        // given
        jobStore.acquireTriggeredJobs(new Date(5000), Integer.MAX_VALUE);
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), new JobData(), (short) 1);
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), new JobData(), (short) 2);
        JobDetail jd3 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), new JobData(), (short) 3);

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);
        jobStore.addJob(jd3, false);

        // when
        List<TriggeredJob> triggeredJobs2 = jobStore.acquireTriggeredJobs(new Date(5000), 2);

        // then
        assertEquals(2, triggeredJobs2.size());
        List<JobDetail> jobDetails = collectJobDetails(triggeredJobs2);
        assertTrue(jobDetails.contains(jd1));
        assertFalse(jobDetails.contains(jd2));
        assertTrue(jobDetails.contains(jd3));
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd1.getJobKey()).get());
        assertEquals(JobState.WAITING, jobStore.getJobState(jd2.getJobKey()).get());
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd3.getJobKey()).get());
    }


    @Test
    public void acquireTriggeredJobsMultipleTimes() throws SchedulerException {
        // given
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(4000)), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), new JobData());

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);

        List<TriggeredJob> triggeredJobs1 = jobStore.acquireTriggeredJobs(new Date(5000), Integer.MAX_VALUE);
        assertTrue(triggeredJobs1.size() >= 2);
        List<JobDetail> jobDetails1 = collectJobDetails(triggeredJobs1);
        assertTrue(jobDetails1.contains(jd1));
        assertTrue(jobDetails1.contains(jd2));

        // when
        List<TriggeredJob> triggeredJobs2 = jobStore.acquireTriggeredJobs(new Date(5000), Integer.MAX_VALUE);

        // then
        assertEquals(0, triggeredJobs2.size());
    }

    @Test
    public void acquireTriggeredCronTriggerJobs() throws SchedulerException, InterruptedException {
        // given
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new CronTrigger("0/1 * * * * *"), new JobData());  // every second
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new CronTrigger("0/1 * * * * * 2030"), new JobData());

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);

        Thread.sleep(2000);  // should have passed the firetime for jd1

        // when
        List<TriggeredJob> triggeredJobs = jobStore.acquireTriggeredJobs(new Date(), Integer.MAX_VALUE);

        // then
        assertTrue(triggeredJobs.size() >= 1);

        List<JobDetail> jobDetails = collectJobDetails(triggeredJobs);
        assertTrue(jobDetails.contains(jd1));
        assertFalse(jobDetails.contains(jd2));
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd1.getJobKey()).get());
        assertEquals(JobState.WAITING, jobStore.getJobState(jd2.getJobKey()).get());
        assertJobDetailEquals(jd1, jobDetails.get(jobDetails.indexOf(jd1)));
    }

    @Test
    public void getJobStateJobDoesntExist() throws SchedulerException, InterruptedException {
        // given
        JobKey jk = new JobKey(getUniqueKey());

        // when
        Optional<JobState> jobState = jobStore.getJobState(jk);

        // then
        assertFalse(jobState.isPresent());
    }
}
