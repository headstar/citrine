package com.tango.citrine.jobstore;



import com.google.common.base.Optional;
import com.tango.citrine.JobAlreadyExistsException;
import com.tango.citrine.JobDetail;
import com.tango.citrine.JobKey;
import com.tango.citrine.SchedulerException;

import java.util.Date;
import java.util.List;

/**
 * Created by per on 3/7/15.
 */
public interface JobStore {

    /**
     * Called before the JobStore is used.
     */
    void initialize();

    boolean addJob(JobDetail jobDetail, boolean replace) throws JobAlreadyExistsException;

    void addJobs(Iterable<JobDetail> jobDetails) throws JobAlreadyExistsException;

    boolean removeJob(JobKey jobKey);

    boolean exists(JobKey jobKey);

    Optional<JobDetail> getJob(JobKey jobKey) throws SchedulerException;

    Optional<JobState> getJobState(JobKey jobKey);

    List<TriggeredJob> acquireTriggeredJobs(Date currentTime, int maxCount) throws SchedulerException;

    void triggeredJobComplete(TriggeredJob job, TriggeredJobCompleteAction action);

    void shutdown();

}
