package com.headstartech.citrine;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.joda.time.DateTime;

import java.util.Date;

/**
 *
 *
 */
public class CronTrigger implements Trigger {

    private final String cronExpression;
    private final ExecutionTime executionTime;

    public CronTrigger(String cronExpression) {
        Preconditions.checkNotNull(cronExpression, "cronExpression cannot be null");
        this.cronExpression = cronExpression;
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
        Cron cron = parser.parse(cronExpression);
        executionTime = ExecutionTime.forCron(cron);
    }

    @Override
    public Optional<Date> nextExecutionTime(TriggerContext triggerContext) {
        Date referenceDate = null;
        if(triggerContext.getLastActualExecutionTime().isPresent()) {
            referenceDate = triggerContext.getLastActualExecutionTime().get();
        } else {
            referenceDate = new Date();
        }
        return Optional.of(executionTime.nextExecution(new DateTime(referenceDate)).toDate());

    }

    public String getCronExpression() {
        return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CronTrigger that = (CronTrigger) o;

        return cronExpression.equals(that.cronExpression);
    }

    @Override
    public int hashCode() {
        return cronExpression.hashCode();
    }

    @Override
    public String toString() {
        return "CronTrigger{" +
                "cronExpression='" + cronExpression + '\'' +
                '}';
    }
}
