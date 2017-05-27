package com.tango.citrine.jobstore.jdbc.jobclassmapper;


import com.tango.citrine.Job;

/**
 * Interface for mapping between a {@link com.tango.citrine.Job} and a <code>String</code>.
 *
 */
public interface JobClassMapper {

    Class<? extends Job> stringToClass(String jobClass) throws ClassNotFoundException;

    String classToString(Class<? extends Job> jobClass);

}
