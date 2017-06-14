package com.headstartech.citrine.jobstore;

import com.headstartech.citrine.JobDetail;

import java.util.Date;

/**
 * Created by per on 12/03/15.
 */
public interface TriggeredJob {

    JobDetail getJobDetail();

    Date scheduledExecutionTime();

    Date actualExecutionTime();
}
