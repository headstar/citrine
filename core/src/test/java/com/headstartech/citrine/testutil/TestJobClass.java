package com.headstartech.citrine.testutil;


import com.headstartech.citrine.Job;
import com.headstartech.citrine.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by per on 27/03/15.
 */
public class TestJobClass implements Job {

    private final Logger logger = LoggerFactory.getLogger(TestJobClass.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        logger.info("Executing {}", TestJobClass.class.getName());
        TestUtil.JobsExecuted jobsExecuted =  TestUtil.getJobsExecuted(jobExecutionContext.getScheduler());
        jobsExecuted.addJob(jobExecutionContext.getTriggeredJob().getJobDetail());
    }
}
