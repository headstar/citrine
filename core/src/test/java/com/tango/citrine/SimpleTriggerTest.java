package com.tango.citrine;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by per on 11/12/15.
 */
public class SimpleTriggerTest {

    @Test(expected = NullPointerException.class)
    public void createWithNullFireTime() {
        // given

        // when
        new SimpleTrigger(null);

        // then exception be thrown
    }

    @Test
    public void firstExecution() {
        // given
        Date executionTime = new Date(5678);

        // when
        Trigger trigger = new SimpleTrigger(executionTime);

        // then
        assertEquals(executionTime, trigger.nextExecutionTime(new DefaultTriggerContext()).get());
    }

    @Test
    public void secondExecution() {
        // given
        Date executionTime = new Date(5678);

        // when
        Trigger trigger = new SimpleTrigger(executionTime);

        // then
        assertFalse(trigger.nextExecutionTime(new DefaultTriggerContext(new Date(1234), new Date(3456))).isPresent());
    }

    @Test
    public void equalsWhenEqualExecutionTime() {
        // given
        Date executionTime = new Date(5678);
        Trigger a = new SimpleTrigger(executionTime);
        Trigger b = new SimpleTrigger(executionTime);

        // when
        boolean res = a.equals(b);

        // then
        assertTrue(res);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void equalsWhenUnequalExecutionTime() {
        // given
        Trigger a = new SimpleTrigger(new Date(5678));
        Trigger b = new SimpleTrigger(new Date(1234));

        // when
        boolean res = a.equals(b);

        // then
        assertFalse(res);
    }

}
