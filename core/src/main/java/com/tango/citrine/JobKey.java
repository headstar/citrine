package com.tango.citrine;

import com.google.common.base.Preconditions;

/**
 * Class identifying a job.
 *
 */
public class JobKey {

    private final String key;

    public JobKey(String key) {
        Preconditions.checkNotNull(key, "key cannot be null");
        Preconditions.checkArgument(!key.isEmpty(), "key cannot be empty");
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobKey jobKey = (JobKey) o;

        if (!key.equals(jobKey.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key;
    }
}
