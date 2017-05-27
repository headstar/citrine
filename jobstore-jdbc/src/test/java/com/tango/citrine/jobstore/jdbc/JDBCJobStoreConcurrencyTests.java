package com.tango.citrine.jobstore.jdbc;

import com.tango.citrine.jobstore.JobStore;
import com.tango.citrine.jobstore.JobStoreConcurrencyTest;

/**
 * Created by per on 10/12/15.
 */
public class JDBCJobStoreConcurrencyTests extends JobStoreConcurrencyTest {

    @Override
    protected JobStore createJobStore() {
        return TestUtils.createJobStore();
    }
}
