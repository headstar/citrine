package com.headstartech.citrine;

import com.google.common.base.Preconditions;

/**
 * Contains the details of a job. A <code>JobDetail</code> is immutable.
 *
 */
public class JobDetail {

    public static final short PRIORITY_NORMAL = 0;
    public static final short PRIORITY_LOW = -10;
    public static final short PRIORITY_HIGH = 10;

    private final JobKey jobKey;
    private final Class<? extends Job> jobClass;
    private final JobData jobData;
    private final Trigger trigger;
    private final short priority;

    public JobDetail(JobKey jobKey, Class<? extends Job> jobClass, Trigger trigger, JobData jobData) {
        this(jobKey, jobClass, trigger, jobData, PRIORITY_NORMAL);
    }

    public JobDetail(JobKey jobKey, Class<? extends Job> jobClass, Trigger trigger, JobData jobData, short priority) {
        Preconditions.checkNotNull(jobKey, "jobKey cannot be null");
        Preconditions.checkNotNull(jobClass, "jobClass cannot be null");
        Preconditions.checkNotNull(jobData, "jobData cannot be null");
        Preconditions.checkNotNull(trigger, "trigger cannot be null");
        this.jobKey = jobKey;
        this.jobClass = jobClass;
        this.jobData = new JobData(jobData);  // defensive copy
        this.trigger = trigger;
        this.priority = priority;
    }

    public JobKey getJobKey() {
        return jobKey;
    }

    public Class<? extends Job> getJobClass() {
        return jobClass;
    }

    public JobData getJobData() {
        return new JobData(jobData);  // defensive copy
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public short getPriority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobDetail jobDetail = (JobDetail) o;

        if (!jobKey.equals(jobDetail.jobKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return jobKey.hashCode();
    }

    @Override
    public String toString() {
        return "JobDetail{" +
                "jobKey=" + jobKey +
                ", jobClass=" + jobClass +
                ", jobData=" + jobData +
                ", trigger=" + trigger +
                '}';
    }
}


