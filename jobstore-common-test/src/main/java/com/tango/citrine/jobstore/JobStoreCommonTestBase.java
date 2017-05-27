package com.tango.citrine.jobstore;


import com.tango.citrine.JobDetail;
import com.tango.citrine.JobKey;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by per on 03/12/15.
 */
public abstract class JobStoreCommonTestBase {

    protected JobStore jobStore;

    protected abstract JobStore createJobStore();

    protected void destroyJobStore() {
        // do nothing
    }

    @Before
    public void setupCommonTestBase() {
        if(jobStore == null) {
            jobStore = createJobStore();
        }
    }

    @After
    public void tearDownCommonTestBase() {
        if(jobStore != null) {
            destroyJobStore();
            jobStore = null;
        }
    }

    protected String getUniqueKey() {
        return UUID.randomUUID().toString();
    }

    protected JobKey getUniqueJobKey() {
        return new JobKey(getUniqueKey());
    }

    protected List<JobDetail> collectJobDetails(Collection<TriggeredJob> triggeredJobs) {
        List<JobDetail> res = new ArrayList<JobDetail>();
        for(TriggeredJob tj : triggeredJobs) {
            res.add(tj.getJobDetail());
        }
        return res;
    }

    protected void assertJobDetailEquals(JobDetail a, JobDetail b) {
        assertEquals(a.getJobKey(), b.getJobKey());
        assertEquals(a.getJobClass(), b.getJobClass());
        assertEquals(a.getJobData(), b.getJobData());
        assertEquals(a.getTrigger(), b.getTrigger());
        assertEquals(a.getPriority(), b.getPriority());
    }

}
