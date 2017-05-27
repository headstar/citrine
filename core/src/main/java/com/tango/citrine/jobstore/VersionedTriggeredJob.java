package com.tango.citrine.jobstore;


import com.tango.citrine.JobDetail;

import java.util.Date;

/**
 * Created by per on 12/03/15.
 */
public class VersionedTriggeredJob implements TriggeredJob {

    private final JobDetail jobDetail;
    private final int expectedVersionIfNoUpdate;
    private final Date scheduledFireTime;
    private final Date actualFireTime;

    public VersionedTriggeredJob(JobDetail jobDetail, int expectedVersionIfNoUpdate, Date scheduledFireTime, Date actualFireTime) {
        this.jobDetail = jobDetail;
        this.expectedVersionIfNoUpdate = expectedVersionIfNoUpdate;
        this.scheduledFireTime = scheduledFireTime;
        this.actualFireTime = actualFireTime;
    }

    @Override
    public JobDetail getJobDetail() {
        return jobDetail;
    }

    public int getExpectedVersionIfNoUpdate() {
        return expectedVersionIfNoUpdate;
    }

    @Override
    public Date scheduledExecutionTime() {
        return scheduledFireTime;
    }

    @Override
    public Date actualExecutionTime() {
        return actualFireTime;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("VersionedTriggeredJob{");
        sb.append("jobDetail=").append(jobDetail);
        sb.append(", expectedVersionIfNoUpdate=").append(expectedVersionIfNoUpdate);
        sb.append(", scheduledFireTime=").append(scheduledFireTime);
        sb.append(", actualFireTime=").append(actualFireTime);
        sb.append('}');
        return sb.toString();
    }
}
