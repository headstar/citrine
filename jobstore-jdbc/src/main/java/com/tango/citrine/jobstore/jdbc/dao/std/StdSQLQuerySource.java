package com.tango.citrine.jobstore.jdbc.dao.std;

import com.tango.citrine.jobstore.jdbc.dao.AbstractQuerySource;

/**
 * Created by per on 10/12/15.
 */
public class StdSQLQuerySource extends AbstractQuerySource {

    private static final String SELECT_TRIGGERED_TEMPLATE = "SELECT * FROM %s WHERE next_execution_time <= :now AND job_state = 'WAITING' ORDER BY next_execution_time ASC, priority DESC LIMIT :limit FOR UPDATE";

    public StdSQLQuerySource(String tableName) {
        super(tableName);

    }

    protected String getSelectTriggeredSQLTemplate() {
        return SELECT_TRIGGERED_TEMPLATE;
    }
}
