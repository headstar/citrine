package com.tango.citrine.jobstore.jdbc.jobcompleter;

import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;

/**
 * Created by per on 08/12/15.
 */
public interface JobCompleter {

    void completeJob(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action);

    void shutdown();
}
