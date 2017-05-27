package com.tango.citrine.jobstore.jdbc.dao;

import com.google.common.base.MoreObjects;
import com.tango.citrine.jobstore.JobState;

import java.util.Date;

/**
 * Created by per on 02/12/15.
 */
public class JDBCJob {

    private String id;
    private String jobClass;
    private short priority;
    private Date nextExecutionTime;
    private JobState jobState;
    private String jobData;
    private String cronExpression;
    private int version;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJobClass() {
        return jobClass;
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public short getPriority() {
        return priority;
    }

    public void setPriority(short priority) {
        this.priority = priority;
    }

    public Date getNextExecutionTime() {
        return nextExecutionTime;
    }

    public void setNextExecutionTime(Date nextFireTime) {
        this.nextExecutionTime = nextFireTime;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public String getJobData() {
        return jobData;
    }

    public void setJobData(String jobData) {
        this.jobData = jobData;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JDBCJob jdbcJob = (JDBCJob) o;

        if (!id.equals(jdbcJob.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("jobClass", jobClass)
                .add("priority", priority)
                .add("nextFireTime", nextExecutionTime)
                .add("jobState", jobState)
                .add("jobData", jobData)
                .add("cronExpression", cronExpression)
                .add("version", version)
                .toString();
    }
}
