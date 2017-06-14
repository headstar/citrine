package com.headstartech.citrine.jobstore.jdbc.jobclassmapper;


import com.headstartech.citrine.Job;

/**
 * Interface for mapping between a {@link com.headstartech.citrine.Job} and a <code>String</code>.
 *
 */
public interface JobClassMapper {

    Class<? extends Job> stringToClass(String jobClass) throws ClassNotFoundException;

    String classToString(Class<? extends Job> jobClass);

}
