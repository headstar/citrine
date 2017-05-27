package com.tango.citrine;

import com.google.common.base.Optional;

import java.util.Date;

/**
 * Created by per on 18/12/15.
 */
public class DefaultTriggerContext implements TriggerContext {

    private final Date lastScheduledExectionTime;
    private final Date lastActualExecutionTime;

    public DefaultTriggerContext() {
        this(null, null);
    }

    public DefaultTriggerContext(Date lastScheduledExectionTime, Date lastActualExecutionTime) {
        this.lastScheduledExectionTime = lastScheduledExectionTime;
        this.lastActualExecutionTime = lastActualExecutionTime;
    }

    @Override
    public Optional<Date> getLastScheduledExecutionTime() {
        return Optional.fromNullable(lastScheduledExectionTime);
    }

    @Override
    public Optional<Date> getLastActualExecutionTime() {
        return Optional.fromNullable(lastActualExecutionTime);
    }
}
