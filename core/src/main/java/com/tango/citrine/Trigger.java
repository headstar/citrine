package com.tango.citrine;

import com.google.common.base.Optional;

import java.util.Date;

/**
 * Interface for triggers.
 *
 * @see SimpleTrigger
 * @see CronTrigger
 */
public interface Trigger {

    Optional<Date> nextExecutionTime(TriggerContext triggerContext);

}
