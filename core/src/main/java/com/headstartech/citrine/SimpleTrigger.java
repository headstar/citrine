package com.headstartech.citrine;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.Date;

/**
 * Class representing a trigger only firing once.
 *
 */
public class SimpleTrigger implements Trigger {

    private final Date executionTime;

    public SimpleTrigger(Date executionTime) {
        Preconditions.checkNotNull(executionTime, "fireTime cannot be null");
        this.executionTime = executionTime;
    }

    @Override
    public Optional<Date> nextExecutionTime(TriggerContext triggerContext) {
        if(triggerContext.getLastActualExecutionTime().isPresent()) {
            return Optional.absent();
        } else {
            return Optional.of(executionTime);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleTrigger that = (SimpleTrigger) o;

        return executionTime.equals(that.executionTime);
    }

    @Override
    public int hashCode() {
        return executionTime.hashCode();
    }

    @Override
    public String toString() {
        return "SimpleTrigger{" +
                "executionTime=" + executionTime +
                '}';
    }
}
