package com.tango.citrine.jobstore.jdbc.dao;

import com.google.common.collect.Lists;
import com.tango.citrine.jobstore.JobState;
import com.tango.citrine.jobstore.TestJobClass1;
import com.tango.citrine.jobstore.TestJobClass2;
import com.tango.citrine.jobstore.jdbc.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by per on 03/12/15.
 */
public class JobStoreDAOImplTest {

    private JobStoreDAO dao;

    @Before
    public void setup() {
        dao = TestUtils.createJobStoreDAO(TestUtils.createDataSource());
    }

    @Test
    public void insertAndGet() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);

        // when
        dao.insert(job);
        JDBCJob getJob = dao.get(job.getId());

        // then
        assertEquals(job.getId(), getJob.getId());
        assertEquals(job.getCronExpression(), getJob.getCronExpression());
        assertEquals(job.getJobClass(), getJob.getJobClass());
        assertEquals(job.getJobData(), getJob.getJobData());
        assertEquals(job.getJobState(), getJob.getJobState());
        assertEquals(job.getNextExecutionTime(), getJob.getNextExecutionTime());
        assertEquals(job.getVersion(), getJob.getVersion());
    }

    @Test
    public void insertAndGetCollection() {
        // given
        JDBCJob job1 = new JDBCJob();
        job1.setId("foo" + UUID.randomUUID().toString());
        job1.setCronExpression("0 0 12 * * ?");
        job1.setJobClass(TestJobClass1.class.getName());
        job1.setJobData("someData");
        job1.setJobState(JobState.WAITING);
        job1.setNextExecutionTime(new Date(17000));
        job1.setVersion(17);

        JDBCJob job2 = new JDBCJob();
        job2.setId("bar" + UUID.randomUUID().toString());
        job2.setCronExpression("0 0 12 * * ?");
        job2.setJobClass(TestJobClass1.class.getName());
        job2.setJobData("some other data");
        job2.setJobState(JobState.WAITING);
        job2.setNextExecutionTime(new Date(18000));
        job2.setVersion(21);

        List<JDBCJob> jobs = new ArrayList<JDBCJob>();
        jobs.add(job1);
        jobs.add(job2);

        // when
        dao.insert(jobs);

        // then
        JDBCJob getJob1 = dao.get(job1.getId());
        assertEquals(job1.getId(), getJob1.getId());
        assertEquals(job1.getCronExpression(), getJob1.getCronExpression());
        assertEquals(job1.getJobClass(), getJob1.getJobClass());
        assertEquals(job1.getJobData(), getJob1.getJobData());
        assertEquals(job1.getJobState(), getJob1.getJobState());
        assertEquals(job1.getNextExecutionTime(), getJob1.getNextExecutionTime());
        assertEquals(job1.getVersion(), getJob1.getVersion());

        JDBCJob getJob2 = dao.get(job2.getId());
        assertEquals(job2.getId(), getJob2.getId());
        assertEquals(job2.getCronExpression(), getJob2.getCronExpression());
        assertEquals(job2.getJobClass(), getJob2.getJobClass());
        assertEquals(job2.getJobData(), getJob2.getJobData());
        assertEquals(job2.getJobState(), getJob2.getJobState());
        assertEquals(job2.getNextExecutionTime(), getJob2.getNextExecutionTime());
        assertEquals(job2.getVersion(), getJob2.getVersion());
    }

    @Test
    public void delete() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);
        dao.insert(job);
        assertTrue(dao.exists(job.getId()));

        // when
        boolean deleteRes = dao.delete(job.getId());

        // then
        assertTrue(deleteRes);
        assertFalse(dao.exists(job.getId()));
    }

    @Test
    public void deleteWithVersionDifferentFromActual() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);
        dao.insert(job);
        assertTrue(dao.exists(job.getId()));

        // when
        boolean deleteRes = dao.delete(job.getId(), 3);

        // then
        assertFalse(deleteRes);
        assertTrue(dao.exists(job.getId()));
    }

    @Test
    public void deleteWithVersionSameAsActual() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);
        dao.insert(job);
        assertTrue(dao.exists(job.getId()));

        // when
        boolean deleteRes = dao.delete(job.getId(), 17);

        // then
        assertTrue(deleteRes);
        assertFalse(dao.exists(job.getId()));
    }

    @Test
    public void updateWithSameVersionAsActual() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);
        dao.insert(job);

        JDBCJob job2 = new JDBCJob();
        job2.setId(job.getId());
        job2.setCronExpression("0 0 13 * * ?");
        job2.setJobClass(TestJobClass2.class.getName());
        job2.setJobData("otherData");
        job2.setJobState(JobState.EXECUTING);
        job2.setNextExecutionTime(new Date(18000));
        job2.setVersion(job.getVersion());

        // when
        boolean res = dao.updateJob(job2);

        // then
        assertTrue(res);
        JDBCJob jobAfterUpdate = dao.get(job.getId());
        assertEquals("0 0 13 * * ?", jobAfterUpdate.getCronExpression());
        assertEquals(TestJobClass2.class.getName(), jobAfterUpdate.getJobClass());
        assertEquals(JobState.EXECUTING, jobAfterUpdate.getJobState());
        assertEquals(new Date(18000), jobAfterUpdate.getNextExecutionTime());
        assertEquals(18, jobAfterUpdate.getVersion());
    }

    @Test
    public void updateWithDifferentVersionFromActual() {
        // given
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(JobState.WAITING);
        job.setNextExecutionTime(new Date(17000));
        job.setVersion(17);
        dao.insert(job);

        JDBCJob job2 = new JDBCJob();
        job2.setId(job.getId());
        job2.setCronExpression("0 0 13 * * ?");
        job2.setJobClass(TestJobClass2.class.getName());
        job2.setJobData("otherData");
        job2.setJobState(JobState.EXECUTING);
        job2.setNextExecutionTime(new Date(18000));
        job2.setVersion(job.getVersion() + 27);

        // when
        boolean res = dao.updateJob(job2);

        // then
        assertFalse(res);
        JDBCJob jobAfterUpdate = dao.get(job.getId());
        assertEquals(job.getCronExpression(), jobAfterUpdate.getCronExpression());
        assertEquals(job.getJobClass(), jobAfterUpdate.getJobClass());
        assertEquals(job.getJobState(), jobAfterUpdate.getJobState());
        assertEquals(job.getNextExecutionTime(), jobAfterUpdate.getNextExecutionTime());
        assertEquals(job.getVersion(), jobAfterUpdate.getVersion());
    }

    @Test
    public void acquireTriggers() {
        // given
        Date now = new Date(5000);
        List<JDBCJob> before = dao.acquireTriggeredJobs(now, Integer.MAX_VALUE);
        JDBCJob job1 = createWithFireTime(JobState.WAITING, new Date(4000));
        JDBCJob job2 = createWithFireTime(JobState.WAITING, new Date(3000));
        JDBCJob job3 = createWithFireTime(JobState.EXECUTING, new Date(4000));
        JDBCJob job4 = createWithFireTime(JobState.EXECUTING, new Date(6000));
        JDBCJob job5 = createWithFireTime(JobState.WAITING, new Date(6000));

        // when
        List<JDBCJob> after = dao.acquireTriggeredJobs(now, Integer.MAX_VALUE);

        // then
        assertEquals(2, after.size() - before.size());
        assertTrue(after.contains(job1));
        assertTrue(after.contains(job2));
        assertTrue(after.indexOf(job2) < after.indexOf(job1)); // order by fire time ascending
    }

    @Test
    public void setAsExecuting() {
        // given
        JDBCJob job1 = createWithFireTime(JobState.WAITING, new Date(4000));
        JDBCJob job2 = createWithFireTime(JobState.WAITING, new Date(3000));

        // when
        dao.setJobsAsExecuting(Lists.newArrayList(job1.getId(), job2.getId()));

        // then
        JDBCJob job1Updated = dao.get(job1.getId());
        JDBCJob job2Updated = dao.get(job1.getId());
        assertEquals(JobState.EXECUTING, job1Updated.getJobState());
        assertEquals(job1.getVersion() + 1, job1Updated.getVersion());
        assertEquals(JobState.EXECUTING, job2Updated.getJobState());
        assertEquals(job2.getVersion() + 1, job2Updated.getVersion());
    }

    private JDBCJob createWithFireTime(JobState jobState, Date fireTime) {
        JDBCJob job = new JDBCJob();
        job.setId(UUID.randomUUID().toString());
        job.setCronExpression("0 0 12 * * ?");
        job.setJobClass(TestJobClass1.class.getName());
        job.setJobData("someData");
        job.setJobState(jobState);
        job.setNextExecutionTime(fireTime);
        job.setVersion(0);
        dao.insert(job);
        return job;
    }
}
