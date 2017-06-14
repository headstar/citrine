package com.headstartech.citrine.core;

import com.headstartech.citrine.JobListener;
import com.headstartech.citrine.ListenerRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of the {@link ListenerRegistry} interface.
 */
public class ListenerRegistryImpl implements ListenerRegistry {

    private List<JobListener> jobListeners = new ArrayList<JobListener>();
    private ReadWriteLock jobListenersLock = new ReentrantReadWriteLock();

    @Override
    public void addJobListener(JobListener jobListener) {
        Lock lock = jobListenersLock.writeLock();
        lock.lock();
        try {
            jobListeners.add(jobListener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeJobListener(JobListener jobListener) {
        Lock lock = jobListenersLock.writeLock();
        lock.lock();
        try {
            jobListeners.remove(jobListener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<JobListener> getJobListeners() {
        Lock lock = jobListenersLock.readLock();
        lock.lock();
        try {
            return java.util.Collections.unmodifiableList(jobListeners);
        } finally {
            lock.unlock();
        }
    }
}
