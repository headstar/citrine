package com.tango.citrine.jobstore.jdbc.dao;

/**
 * Created by per on 09/12/15.
 */
public interface QuerySource {

    String getInsertSQL();
    String getDeleteSQL();
    String getDeleteWithVersionSQL();
    String getSelectSQL();
    String getSelectExistsSQL();
    String getUpdateSQL();
    String getSelectTriggeredSQL();
    String getSetAsExecutingSQL();

    String getIdColumn();
    String getJobStateColumn();
    String getNextFireTimeColumn();
    String getJobClassColumn();
    String getCronExpressionColumn();
    String getJobDataColumn();
    String getVersionColumn();
    String getPriorityColumn();

    String getIdParameter();
    String getJobStateParameter();
    String getNextFireTimeParameter();
    String getJobClassParameter();
    String getCronExpressionParameter();
    String getJobDataParameter();
    String getVersionParameter();
    String getPriorityParameter();
    String getNowParameter();
    String getLimitParameter();
    String getIdsParameter();

}
