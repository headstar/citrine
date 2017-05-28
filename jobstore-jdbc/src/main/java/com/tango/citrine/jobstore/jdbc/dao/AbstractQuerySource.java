package com.tango.citrine.jobstore.jdbc.dao;

/**
 * Created by per on 09/12/15.
 */
public abstract class AbstractQuerySource implements QuerySource {

    private static final String INSERT_JOB_SQL_TEMPLATE = "INSERT INTO %s (id, created_time, updated_time, job_state, next_execution_time, job_class, priority, job_data, cron_expression, version) VALUES (:id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :job_state, :next_execution_time, :job_class, :priority, :job_data, :cron_expression, :version)";
    private static final String DELETE_JOB_SQL_TEMPLATE = "DELETE FROM %s WHERE id = :id";
    private static final String DELETE_JOB_WITH_VERSION_SQL_TEMPLATE = "DELETE FROM %s WHERE id = :id AND version = :version";
    private static final String SELECT_JOB_SQL_TEMPLATE = "SELECT * FROM %s WHERE id = :id";
    private static final String SELECT_JOB_EXISTS_SQL_TEMPLATE = "SELECT COUNT(id) FROM %s WHERE id = :id";
    private static final String UPDATE_JOB_SQL_TEMPLATE = "UPDATE %s SET updated_time = CURRENT_TIMESTAMP, job_state = :job_state, next_execution_time = :next_execution_time, cron_expression = :cron_expression, job_class = :job_class, priority = :priority, job_data = :job_data, version = version + 1 WHERE id = :id AND version = :version";
    private static final String SET_AS_EXECUTING_SQL_TEMPLATE = "UPDATE %s SET updated_time = NOW(), job_state = 'EXECUTING', version = version + 1 WHERE id IN(:ids)";

    private final String insertSQL;
    private final String deleteSQL;
    private final String deleteWithVersionSQL;
    private final String selectSQL;
    private final String selectExistsSQL;
    private final String updateSQL;
    private final String selectTriggeredSQL;
    private final String setAsExecutingSQL;

    public AbstractQuerySource(String tableName) {
        insertSQL = createInsertSQL(tableName);
        deleteSQL = createDeleteSQL(tableName);
        deleteWithVersionSQL = createDeleteWithVersionSQL(tableName);
        selectSQL = createSelectSQL(tableName);
        selectExistsSQL = createSelectExistsSQL(tableName);
        updateSQL = createUpdateSQL(tableName);
        selectTriggeredSQL = createSelectTriggeredSQL(tableName);
        setAsExecutingSQL = createSetAsExecutingSQL(tableName);
    }

    protected String createInsertSQL(String tableName) {
        return String.format(getInsertSQLTemplate(), tableName);
    }

    protected String createDeleteSQL(String tableName) {
        return String.format(getDeleteSQLTemplate(), tableName);
    }

    protected String createDeleteWithVersionSQL(String tableName) {
        return String.format(getDeleteWithVersionSQLTemplate(), tableName);
    }

    protected String createSelectSQL(String tableName) {
        return String.format(getSelectSQLTemplate(), tableName);
    }

    protected String createSelectExistsSQL(String tableName) {
        return String.format(getSelectExistsSQLTemplate(),tableName);
    }

    protected String createUpdateSQL(String tableName) {
        return String.format(getUpdateSQLTemplate(), tableName);
    }

    protected String createSelectTriggeredSQL(String tableName) {
        return String.format(getSelectTriggeredSQLTemplate(), tableName);
    }

    protected String createSetAsExecutingSQL(String tableName) {
        return String.format(getSetAsExecutingSQLTemplate(), tableName);
    }

    protected String getInsertSQLTemplate() {
        return INSERT_JOB_SQL_TEMPLATE;
    }

    protected String getDeleteSQLTemplate() {
        return DELETE_JOB_SQL_TEMPLATE;
    }

    protected String getDeleteWithVersionSQLTemplate() {
        return DELETE_JOB_WITH_VERSION_SQL_TEMPLATE;
    }

    protected String getSelectSQLTemplate() {
        return SELECT_JOB_SQL_TEMPLATE;
    }

    protected String getSelectExistsSQLTemplate() {
        return SELECT_JOB_EXISTS_SQL_TEMPLATE;
    }

    protected String getUpdateSQLTemplate() {
        return UPDATE_JOB_SQL_TEMPLATE;
    }

    protected String getSetAsExecutingSQLTemplate() {
        return SET_AS_EXECUTING_SQL_TEMPLATE;
    }

    protected abstract String getSelectTriggeredSQLTemplate();

    @Override
    public String getSelectTriggeredSQL() {
        return selectTriggeredSQL;
    }

    @Override
    public String getInsertSQL() {
        return insertSQL;
    }

    @Override
    public String getDeleteSQL() {
        return deleteSQL;
    }

    @Override
    public String getDeleteWithVersionSQL() {
        return deleteWithVersionSQL;
    }

    @Override
    public String getSelectSQL() {
        return selectSQL;
    }

    @Override
    public String getSelectExistsSQL() {
        return selectExistsSQL;
    }

    @Override
    public String getUpdateSQL() {
        return updateSQL;
    }

    @Override
    public String getSetAsExecutingSQL() {
        return setAsExecutingSQL;
    }

    @Override
    public String getIdColumn() {
        return "id";
    }

    @Override
    public String getJobStateColumn() {
        return "job_state";
    }

    @Override
    public String getNextFireTimeColumn() {
        return "next_execution_time";
    }

    @Override
    public String getJobClassColumn() {
        return "job_class";
    }

    @Override
    public String getCronExpressionColumn() {
        return "cron_expression";
    }

    @Override
    public String getJobDataColumn() {
        return "job_data";
    }

    @Override
    public String getVersionColumn() {
        return "version";
    }

    @Override
    public String getPriorityColumn() {
        return "priority";
    }

    @Override
    public String getIdParameter() {
        return getIdColumn();
    }

    @Override
    public String getJobStateParameter() {
        return getJobStateColumn();
    }

    @Override
    public String getNextFireTimeParameter() {
        return getNextFireTimeColumn();
    }

    @Override
    public String getJobClassParameter() {
        return getJobClassColumn();
    }

    @Override
    public String getCronExpressionParameter() {
        return getCronExpressionColumn();
    }

    @Override
    public String getJobDataParameter() {
        return getJobDataColumn();
    }

    @Override
    public String getVersionParameter() {
        return getVersionColumn();
    }

    @Override
    public String getNowParameter() {
        return "now";
    }

    @Override
    public String getLimitParameter() {
        return "limit";
    }

    @Override
    public String getPriorityParameter() {
        return getPriorityColumn();
    }

    @Override
    public String getIdsParameter() {
        return "ids";
    }

}
