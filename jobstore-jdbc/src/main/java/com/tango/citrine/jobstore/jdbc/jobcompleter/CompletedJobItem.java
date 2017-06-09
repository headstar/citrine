package com.tango.citrine.jobstore.jdbc.jobcompleter;

import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;

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
