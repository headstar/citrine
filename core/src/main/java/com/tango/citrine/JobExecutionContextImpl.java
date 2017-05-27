package com.tango.citrine;

import com.tango.citrine.jobstore.TriggeredJob;

/**
 * Implementation of the <code>JobExecutionContext</code> interface.
 */
public class JobExecutionContextImpl implements JobExecutionContext {

    private final TriggeredJob triggeredJob;
    private final Scheduler scheduler;

    public JobExecutionContextImpl(TriggeredJob triggeredJob, Scheduler scheduler) {
        this.triggeredJob = triggeredJob;
        this.scheduler = scheduler;
    }

    @Override
    public TriggeredJob getTriggeredJob() {
        return triggeredJob;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

}
