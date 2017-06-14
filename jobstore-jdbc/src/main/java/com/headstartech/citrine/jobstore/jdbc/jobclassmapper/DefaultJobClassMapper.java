package com.headstartech.citrine.jobstore.jdbc.jobclassmapper;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.headstartech.citrine.Job;

/**
 * A map based job class mapper.
 *
 * If there is no entry for a <code>Class></code>, <code>Class.getName()</code> is used.
 */
public class DefaultJobClassMapper implements JobClassMapper {

    private final BiMap<Class<? extends Job>, String> map;

    public DefaultJobClassMapper(BiMap<Class<? extends Job>, String> map) {
        this.map = ImmutableBiMap.copyOf(map);
    }

    public DefaultJobClassMapper() {
        this.map = ImmutableBiMap.of();
    }

    @Override
    public Class<? extends Job> stringToClass(String jobClassString) throws ClassNotFoundException {
        Class<? extends Job> jobClass =  map.inverse().get(jobClassString);
        if(jobClass != null) {
            return jobClass;
        }
        Class<?> clazz = Class.forName(jobClassString);
        return clazz.asSubclass(Job.class);
    }

    @Override
    public String classToString(Class<? extends Job> jobClass) {
        String jobClassString = map.get(jobClass);
        if(jobClassString != null) {
            return jobClassString;
        }
        return jobClass.getName();
    }
}
