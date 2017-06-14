package com.headstartech.citrine;

import java.util.List;

/**
 * Interface for the listener registry.
 *
 * Listener registration order is preserved, and hence notification of listeners will be in the order in which they were registered.
 */
public interface ListenerRegistry {

    void addJobListener(JobListener jobListener);

    void removeJobListener(JobListener jobListener);

    List<JobListener> getJobListeners();
}
