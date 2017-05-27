package com.tango.citrine.core;

import com.tango.citrine.JobExecutionContext;
import com.tango.citrine.JobListener;
import com.tango.citrine.ListenerRegistry;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by per on 11/12/15.
 */
public class ListenerRegistryImplTest {

    @Test
    public void GetJobListenersEmpty() {
        // given
        ListenerRegistry listenerRegistry = new ListenerRegistryImpl();

        // when
        List<JobListener> listeners = listenerRegistry.getJobListeners();

        // then
        assertTrue(listeners.isEmpty());
    }

    @Test
    public void Add() {
        // given
        ListenerRegistry listenerRegistry = new ListenerRegistryImpl();
        JobListener jl = new TestJobListener();

        // when
        listenerRegistry.addJobListener(jl);

        // then
        assertTrue(listenerRegistry.getJobListeners().contains(jl));
    }

    @Test
    public void Remove() {
        // given
        ListenerRegistry listenerRegistry = new ListenerRegistryImpl();
        JobListener jl = new TestJobListener();
        listenerRegistry.addJobListener(jl);

        // when
        listenerRegistry.removeJobListener(jl);

        // then
        assertFalse(listenerRegistry.getJobListeners().contains(jl));
    }


    private static class TestJobListener implements JobListener {

        @Override
        public void jobToBeExecuted(JobExecutionContext context) {

        }
    }
}
