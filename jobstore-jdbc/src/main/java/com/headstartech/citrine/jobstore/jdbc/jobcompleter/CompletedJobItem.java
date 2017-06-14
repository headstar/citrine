package com.headstartech.citrine.jobstore.jdbc.jobcompleter;

import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import com.headstartech.citrine.jobstore.VersionedTriggeredJob;

/**
 * @author Per Johansson
 */
public class CompletedJobItem {

    private final VersionedTriggeredJob triggeredJob;
    private final TriggeredJobCompleteAction action;

    public CompletedJobItem(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action) {
        this.triggeredJob = triggeredJob;
        this.action = action;
    }

    public VersionedTriggeredJob getTriggeredJob() {
        return triggeredJob;
    }

    public TriggeredJobCompleteAction getAction() {
        return action;
    }
}
