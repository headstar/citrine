package com.headstartech.citrine.jobstore.jdbc.jobcompleter;

import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import com.headstartech.citrine.jobstore.VersionedTriggeredJob;

/**
 * Created by per on 08/12/15.
 */
public interface JobCompleter {

    void completeJob(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action);

    void shutdown();
}
