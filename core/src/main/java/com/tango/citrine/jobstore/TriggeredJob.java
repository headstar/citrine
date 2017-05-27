package com.tango.citrine.jobstore;

import com.tango.citrine.JobDetail;

import java.util.Date;

/**
 * Created by per on 12/03/15.
 */
public interface TriggeredJob {

    JobDetail getJobDetail();

    Date scheduledExecutionTime();

    Date actualExecutionTime();
}
