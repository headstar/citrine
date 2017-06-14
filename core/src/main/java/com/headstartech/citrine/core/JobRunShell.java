package com.headstartech.citrine.core;

import com.headstartech.citrine.*;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.TriggeredJob;
import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by per on 3/7/15.
 */
public class JobRunShell implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(JobRunShell.class);

    private final JobStore jobStore;
    private final JobExecutionContext jobExecutionContext;
    private final TriggeredJob triggeredJob;
    private final Job job;
    private final Scheduler scheduler;

    JobRunShell(TriggeredJob triggeredJob, JobStore jobStore, Scheduler scheduler) throws SchedulerException {
        this.jobStore = jobStore;
        this.triggeredJob = triggeredJob;
        this.job = createJobInstance(triggeredJob.getJobDetail());
        this.scheduler = scheduler;
        jobExecutionContext = new JobExecutionContextImpl(triggeredJob, scheduler);
    }

    @Override
    public void run() {
        JobDetail jobDetail = jobExecutionContext.getTriggeredJob().getJobDetail();
        JobKey jobKey = jobDetail.getJobKey();

        notifyJobListeners();
        try {
            logger.debug("Calling execute on job: jobKey={} ", jobKey);
            job.execute(jobExecutionContext);
        } catch(Throwable t) {
            logger.error(String.format("Job threw unhandled exception: jobKey=%s", jobKey), t);
        }

        jobStore.triggeredJobComplete(triggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
    }

    private void notifyJobListeners() {
        List<JobListener> jobListeners = scheduler.getListenerRegistry().getJobListeners();
        for(JobListener jl: jobListeners) {
            try {
                jl.jobToBeExecuted(jobExecutionContext);
            } catch (Exception e) {
                logger.warn(String.format("JobListener threw exception: jobListener=%s", jl), e);
            }
        }
    }

    private Job createJobInstance(JobDetail jobDetail) throws SchedulerException {
        Class<? extends Job> jobClass = jobDetail.getJobClass();
        try {
            logger.debug("Creating instance of Job: jobKey={}, class={}", jobDetail.getJobKey().getKey(), jobClass.getName());
            return jobClass.newInstance();
        } catch (Exception e) {
            SchedulerException se = new SchedulerException("Failed to instantiate class '" + jobClass.getName() + "'", e);
            throw se;
        }
    }
}
