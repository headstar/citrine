package com.headstartech.citrine;

import com.headstartech.citrine.jobstore.TriggeredJob;

/**
 * Context information available to a {@link Job}.
 */
public interface JobExecutionContext {

    TriggeredJob getTriggeredJob();

    Scheduler getScheduler();

}
