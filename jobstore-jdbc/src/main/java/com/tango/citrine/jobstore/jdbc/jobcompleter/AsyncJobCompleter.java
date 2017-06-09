package com.tango.citrine.jobstore.jdbc.jobcompleter;

import com.headstartech.burro.WorkQueue;
import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Per Johansson
 */
public class AsyncJobCompleter implements JobCompleter {

    private static final Logger logger = LoggerFactory.getLogger(AsyncJobCompleter.class);

    private final WorkQueue<CompletedJobItem> workQueue;

    public AsyncJobCompleter(WorkQueue<CompletedJobItem> workQueue) {
        this.workQueue = workQueue;
        workQueue.start();
    }

    @Override
    public void completeJob(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action) {
        CompletedJobItem item = new CompletedJobItem(triggeredJob, action);
        try {
            boolean added;
            do {
                added = workQueue.add(item, 1000, TimeUnit.MILLISECONDS);
                if(!added) {
                    logger.debug("Work queue full, trying again...");
                }
            } while(!added);
        } catch (InterruptedException e) {
            // TODO
        }
    }

    @Override
    public void shutdown() {
        workQueue.shutdown();
    }
}
