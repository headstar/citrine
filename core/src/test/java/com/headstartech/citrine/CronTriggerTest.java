package com.headstartech.citrine;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by per on 11/12/15.
 */
public class CronTriggerTest {

    @Test(expected = NullPointerException.class)
    public void createWithNullFireTime() {
        // given

        // when
        new CronTrigger(null);

        // then exception be thrown
    }

    @Test
    public void create() {
        // given
        String cronExpression = "0/1 * * * * *";

        // when
        CronTrigger trigger = new CronTrigger(cronExpression);

        // then
        assertNotNull(trigger.nextExecutionTime(new DefaultTriggerContext(null, null)));
        assertEquals(cronExpression, trigger.getCronExpression());
    }

    @Test
    public void equalsWhenEqualFireTime() {
        // given
        String cronExpression = "0/1 * * * * *";

        CronTrigger a = new CronTrigger(cronExpression);
        CronTrigger b = new CronTrigger(cronExpression);

        // when
        boolean res = a.equals(b);

        // then
        assertTrue(res);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void equalsWhenUnequalFireTime() {
        // given
        CronTrigger a = new CronTrigger("0/1 * * * * *");
        CronTrigger b = new CronTrigger("0/2 * * * * *");

        // when
        boolean res = a.equals(b);

        // then
        assertFalse(res);
    }
}
