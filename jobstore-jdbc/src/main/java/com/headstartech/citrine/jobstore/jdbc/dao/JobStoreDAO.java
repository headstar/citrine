package com.headstartech.citrine.jobstore.jdbc.dao;

import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by per on 02/12/15.
 */
public interface JobStoreDAO {

    void insert(JDBCJob job);

    void insert(Iterable<JDBCJob> jobs);

    boolean updateJob(JDBCJob job);

    boolean delete(String jobId);

    boolean delete(String jobId, int version);

    void delete(List<String> jobIds, int version);

    boolean exists(String jobId);

    JDBCJob get(String jobId);

    List<JDBCJob> acquireTriggeredJobs(Date referenceTime, int maxCount);

    void setJobsAsExecuting(Collection<String> jobIds);
}
