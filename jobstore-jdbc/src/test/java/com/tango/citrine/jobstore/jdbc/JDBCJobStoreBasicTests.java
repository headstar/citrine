package com.tango.citrine.jobstore.jdbc;

import com.tango.citrine.*;
import com.tango.citrine.jobstore.*;
import com.tango.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.tango.citrine.jobstore.jdbc.jobcompleter.JobCompleter;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by per on 03/12/15.
 */
public class JDBCJobStoreBasicTests extends BasicJobStoreTests {

    @Override
    protected JobStore createJobStore() {
        return TestUtils.createJobStore();
    }

    @Test
    public void getUnknownJobClass() {
        // given
        JobClassMapper exceptionThrowingMapper = new JobClassMapper() {
            @Override
            public Class<? extends Job> stringToClass(String jobClass) throws ClassNotFoundException {
                throw new ClassNotFoundException("");
            }

            @Override
            public String classToString(Class<? extends Job> jobClass) {
                return jobClass.getName();
            }
        };

        JobStore jobStore = TestUtils.createJobStore(TestUtils.createDataSource(), exceptionThrowingMapper);
        JobKey jk = new JobKey(getUniqueKey());
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date(10000)), new JobData());
        jobStore.addJob(jd, false);

        // when
        try {
            jobStore.getJob(jk);
            fail("expected exception!");
        } catch(SchedulerException e) {

            // then
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void acquireUnknownJobClass() {
        // given
        JobClassMapper exceptionThrowingMapper = new JobClassMapper() {
            @Override
            public Class<? extends Job> stringToClass(String jobClass) throws ClassNotFoundException {
                if(jobClass.equals(TestJobClass2.class.getName())) {
                    throw new ClassNotFoundException("TEST EXCEPTION!");
                }
                Class<?> clazz = Class.forName(jobClass);
                return clazz.asSubclass(Job.class);
            }

            @Override
            public String classToString(Class<? extends Job> jobClass) {
                return jobClass.getName();
            }
        };

        JobStore jobStore = TestUtils.createJobStore(TestUtils.createDataSource(), exceptionThrowingMapper);
        JobDetail jd1 = new JobDetail(getUniqueJobKey(), TestJobClass1.class, new SimpleTrigger(new Date(3000)), new JobData());
        JobDetail jd2 = new JobDetail(getUniqueJobKey(), TestJobClass2.class, new SimpleTrigger(new Date(4000)), new JobData());

        jobStore.addJob(jd1, false);
        jobStore.addJob(jd2, false);

        // when
        List<TriggeredJob> triggeredJobs = jobStore.acquireTriggeredJobs(new Date(5000), Integer.MAX_VALUE);

        // then
        List<JobDetail> jobDetails = collectJobDetails(triggeredJobs);
        assertTrue(jobDetails.contains(jd1));
        assertFalse(jobDetails.contains(jd2));
        assertEquals(JobState.EXECUTING, jobStore.getJobState(jd1.getJobKey()).get());
        assertEquals(JobState.ERROR, jobStore.getJobState(jd2.getJobKey()).get());
    }

    @Test
    public void jobCompleterThrows() {
        // given
        JobCompleter exceptionThrowingCompleter = new JobCompleter() {
            @Override
            public void completeJob(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action) {
                throw new RuntimeException("EXCEPTION!");
            }

            @Override
            public void shutdown() {

            }
        };

        JobKey jk = getUniqueJobKey();
        JobDetail jd = new JobDetail(jk, TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
        JobStore jobStore = TestUtils.createJobStore(TestUtils.createDataSource(), exceptionThrowingCompleter);
        TriggeredJob triggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());

        // when
        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);

        // then ... no exception should be thrown out of #triggeredJobComplete
    }

}
