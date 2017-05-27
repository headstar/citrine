package com.tango.citrine.jobstore.jdbc;

import com.tango.citrine.jobstore.JobCompletionTests;
import com.tango.citrine.jobstore.JobStore;

/**
 * Created by per on 08/12/15.
 */
public class JDBCJobStoreJobCompletionTests extends JobCompletionTests {

    @Override
    protected JobStore createJobStore() {
        return TestUtils.createJobStore();
    }
}
