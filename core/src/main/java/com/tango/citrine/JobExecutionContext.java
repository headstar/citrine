package com.tango.citrine;

import com.tango.citrine.jobstore.TriggeredJob;

/**
 * Context information available to a {@link Job}.
 */
public interface JobExecutionContext {

    TriggeredJob getTriggeredJob();

    Scheduler getScheduler();

}
