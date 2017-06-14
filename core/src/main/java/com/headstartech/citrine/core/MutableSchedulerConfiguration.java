package com.headstartech.citrine.core;

import com.google.common.base.Preconditions;

/**
 * Created by per on 30/03/15.
 */
public class MutableSchedulerConfiguration implements SchedulerConfiguration {

    private volatile int idleWaitTime;
    private volatile int maxBatchSize;

    public MutableSchedulerConfiguration(int idleWaitTime, int maxBatchSize) {
        setIdleWaitTime(idleWaitTime);
        setMaxBatchSize(maxBatchSize);
    }

    public int getIdleWaitTime() {
        return idleWaitTime;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        Preconditions.checkArgument(maxBatchSize >= 0, "maxBatchSize cannot be < 0");
        this.maxBatchSize = maxBatchSize;
    }

    public void setIdleWaitTime(int idleWaitTime) {
        Preconditions.checkArgument(idleWaitTime >=0, "idleWaitTime cannot be < 0");
        this.idleWaitTime = idleWaitTime;
    }
}
