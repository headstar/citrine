package com.headstartech.citrine.jobstore.jdbc;

import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.JobStoreConcurrencyTest;

/**
 * Created by per on 10/12/15.
 */
public class JDBCJobStoreConcurrencyTests extends JobStoreConcurrencyTest {

    @Override
    protected JobStore createJobStore() {
        return TestUtils.createJobStore();
    }
}
