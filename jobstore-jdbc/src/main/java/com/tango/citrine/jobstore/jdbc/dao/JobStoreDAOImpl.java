package com.tango.citrine.jobstore.jdbc.dao;

import com.tango.citrine.jobstore.JobState;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by per on 02/12/15.
 */
public class JobStoreDAOImpl implements JobStoreDAO {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final QuerySource querySource;
    private final RowMapper<JDBCJob> rowMapper;

    public JobStoreDAOImpl(NamedParameterJdbcTemplate namedParameterJdbcTemplate, QuerySource querySource) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.querySource = querySource;
        this.rowMapper = new JDBCJobRowMappper(querySource);
    }

    @Override
    public void insert(JDBCJob job) {
        namedParameterJdbcTemplate.update(querySource.getInsertSQL(), getInsertParameterSource(job));
    }

    @Override
    public void insert(Iterable<JDBCJob> jobs) {
        List<SqlParameterSource> sqlParameterSources = new ArrayList<SqlParameterSource>();
        for(JDBCJob job : jobs) {
            sqlParameterSources.add(getInsertParameterSource(job));
        }
        SqlParameterSource[] batch = new SqlParameterSource[sqlParameterSources.size()];
        namedParameterJdbcTemplate.batchUpdate(querySource.getInsertSQL(), sqlParameterSources.toArray(batch));
    }

    @Override
    public boolean updateJob(JDBCJob job) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), job.getId());
        namedParameters.addValue(querySource.getJobStateParameter(), job.getJobState().name());
        namedParameters.addValue(querySource.getNextFireTimeParameter(), job.getNextExecutionTime());
        namedParameters.addValue(querySource.getJobClassParameter(), job.getJobClass());
        namedParameters.addValue(querySource.getPriorityParameter(), job.getPriority());
        namedParameters.addValue(querySource.getJobDataParameter(), job.getJobData());
        namedParameters.addValue(querySource.getCronExpressionParameter(), job.getCronExpression());
        namedParameters.addValue(querySource.getVersionParameter(), job.getVersion());
        namedParameters.addValue(querySource.getNowParameter(), new Date());

        return namedParameterJdbcTemplate.update(querySource.getUpdateSQL(), namedParameters) == 1;
    }

    @Override
    public boolean delete(String jobId) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), jobId);

        return namedParameterJdbcTemplate.update(querySource.getDeleteSQL(), namedParameters) > 0;
    }

    @Override
    public boolean delete(String jobId, int version) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), jobId);
        namedParameters.addValue(querySource.getVersionParameter(), version);

        return namedParameterJdbcTemplate.update(querySource.getDeleteWithVersionSQL(), namedParameters) > 0;
    }

    @Override
    public void delete(List<String> jobIds, int version) {
        if(!jobIds.isEmpty()) {
            MapSqlParameterSource namedParameters = new MapSqlParameterSource();
            namedParameters.addValue(querySource.getJobsParameter(), jobIds);
            namedParameters.addValue(querySource.getVersionParameter(), version);

            namedParameterJdbcTemplate.update(querySource.getDeleteListWithVersionSQL(), namedParameters);
        }
    }

    @Override
    public boolean exists(String jobId) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), jobId);

        return namedParameterJdbcTemplate.queryForObject(querySource.getSelectExistsSQL(), namedParameters, Integer.class) == 1;
    }

    @Override
    public JDBCJob get(String jobId) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), jobId);

        List<JDBCJob> res = namedParameterJdbcTemplate.query(querySource.getSelectSQL(), namedParameters, rowMapper);
        if(!res.isEmpty()) {
            return res.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<JDBCJob> acquireTriggeredJobs(Date referenceTime, int maxCount) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getNowParameter(), referenceTime);
        namedParameters.addValue(querySource.getLimitParameter(), maxCount);

        return namedParameterJdbcTemplate.query(querySource.getSelectTriggeredSQL(), namedParameters, rowMapper);
    }

    @Override
    public void setJobsAsExecuting(Collection<String> jobIds) {
        if(jobIds.isEmpty()) {
            return;
        }
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdsParameter(), jobIds);
        namedParameters.addValue(querySource.getNowParameter(), new Date());
        namedParameterJdbcTemplate.update(querySource.getSetAsExecutingSQL(), namedParameters);
    }

    private static class JDBCJobRowMappper implements RowMapper<JDBCJob> {

        private final QuerySource querySource;

        public JDBCJobRowMappper(QuerySource querySource) {
            this.querySource = querySource;
        }

        @Override
        public JDBCJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            JDBCJob job = new JDBCJob();
            job.setId(rs.getString(querySource.getIdColumn()));
            job.setJobState(JobState.valueOf(rs.getString(querySource.getJobStateColumn())));
            job.setNextExecutionTime(new Date(rs.getTimestamp(querySource.getNextFireTimeColumn()).getTime()));  // make sure we assign a java.util.Date and not a java.sql.Time (new java.util.Date(17).equals(new java.sql.Time(17)) -> false
            job.setJobClass(rs.getString(querySource.getJobClassColumn()));
            job.setPriority(rs.getShort(querySource.getPriorityColumn()));
            job.setJobData(rs.getString(querySource.getJobDataColumn()));
            job.setCronExpression(rs.getString(querySource.getCronExpressionColumn()));
            job.setVersion(rs.getInt(querySource.getVersionColumn()));
            return job;
        }
    };

    private SqlParameterSource getInsertParameterSource(JDBCJob job) {
        MapSqlParameterSource namedParameters = new MapSqlParameterSource();
        namedParameters.addValue(querySource.getIdParameter(), job.getId());
        namedParameters.addValue(querySource.getJobStateParameter(), job.getJobState().name());
        namedParameters.addValue(querySource.getNextFireTimeParameter(), job.getNextExecutionTime());
        namedParameters.addValue(querySource.getJobClassParameter(), job.getJobClass());
        namedParameters.addValue(querySource.getPriorityParameter(), job.getPriority());
        namedParameters.addValue(querySource.getJobDataParameter(), job.getJobData());
        namedParameters.addValue(querySource.getCronExpressionParameter(), job.getCronExpression());
        namedParameters.addValue(querySource.getVersionParameter(), job.getVersion());
        namedParameters.addValue(querySource.getNowParameter(), new Date());
        return namedParameters;
    }
}
