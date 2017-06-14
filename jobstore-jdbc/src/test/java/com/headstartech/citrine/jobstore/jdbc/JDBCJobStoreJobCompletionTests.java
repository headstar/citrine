package com.headstartech.citrine.jobstore.jdbc;

import com.headstartech.citrine.jobstore.JobCompletionTests;
import com.headstartech.citrine.jobstore.JobStore;

/**
 * Created by per on 08/12/15.
 */
public class JDBCJobStoreJobCompletionTests extends JobCompletionTests {

    @Override
    protected JobStore createJobStore() {
        return TestUtils.createJobStore();
    }
}
