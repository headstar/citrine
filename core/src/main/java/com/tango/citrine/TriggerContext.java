package com.tango.citrine;

import com.google.common.base.Optional;

import java.util.Date;

/**
 * Created by per on 18/12/15.
 */
public interface TriggerContext {

    Optional<Date> getLastScheduledExecutionTime();

    Optional<Date> getLastActualExecutionTime();
}
