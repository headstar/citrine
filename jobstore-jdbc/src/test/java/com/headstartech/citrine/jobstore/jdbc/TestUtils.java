package com.headstartech.citrine.jobstore.jdbc;

import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.jdbc.dao.JDBCJob;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAO;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAOImpl;
import com.headstartech.citrine.jobstore.jdbc.dao.std.StdSQLQuerySource;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.DefaultJobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.DefaultJobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.JobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobdata.DefaultJobDataEncoderDecoder;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by per on 11/12/15.
 */
public class TestUtils {
    private static final String H2_JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String H2_USER = "sa";
    private static final String H2_PASSWORD = "";
    private static final Charset UTF_8 = null;

    public static JobStore createJobStore() {
        return createJobStore(createDataSource());
    }

    public static JobStore createJobStore(DataSource dataSource) {
       return createJobStore(dataSource, new DefaultJobClassMapper());
    }

    private static boolean h2DatabaseCreated = false;

    static void createH2Database() {
        try {
            RunScript.execute(H2_JDBC_URL, H2_USER, H2_PASSWORD, "classpath:sql/h2-schema.sql", UTF_8, false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataSource createDataSource() {
        if(!h2DatabaseCreated) {
            createH2Database();
            h2DatabaseCreated = true;
        }
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(H2_JDBC_URL);
        dataSource.setUser(H2_USER);
        dataSource.setPassword(H2_PASSWORD);
        return dataSource;
    }

    public static TransactionOperations createTransactionOperations(DataSource dataSource) {
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        TransactionOperations transactionOperations = new TransactionTemplate(transactionManager);
        return transactionOperations;
    }

    public static JobStore createJobStore(DataSource dataSource, JobClassMapper jobClassMapper) {
        TransactionOperations transactionOperations = createTransactionOperations(dataSource);
        JobStoreDAO dao = createJobStoreDAO(dataSource);
        return new JDBCJobStore(transactionOperations, dao, jobClassMapper, new DefaultJobDataEncoderDecoder(),
                new DefaultJobCompleter(dao, transactionOperations, createRetryOperations()));
    }

    public static JobStore createJobStore(DataSource dataSource, JobCompleter jobCompleter) {
        JobStoreDAO dao = new JobStoreDAOImpl(new NamedParameterJdbcTemplate(dataSource), new StdSQLQuerySource("job"));
        TransactionOperations transactionOperations = createTransactionOperations(dataSource);
        return new JDBCJobStore(transactionOperations, dao, new DefaultJobClassMapper(), new DefaultJobDataEncoderDecoder(),
                jobCompleter);
    }

    public static JobStoreDAO createJobStoreDAO(DataSource dataSource) {
        JobStoreDAO dao = new JobStoreDAOImpl(new NamedParameterJdbcTemplate(dataSource), new StdSQLQuerySource("job"));
        return dao;
    }

    public static class ExceptionThrowingTransactionOperations implements TransactionOperations {

        private final RuntimeException e;

        public ExceptionThrowingTransactionOperations(RuntimeException e) {
            this.e = e;
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) throws TransactionException {
            throw e;
        }
    }

    public static class DummyDAO implements JobStoreDAO {
        @Override
        public void insert(JDBCJob job) {

        }

        @Override
        public void insert(Iterable<JDBCJob> jobs) {

        }

        @Override
        public boolean updateJob(JDBCJob job) {
            return false;
        }

        @Override
        public boolean delete(String jobId) {
            return false;
        }

        @Override
        public boolean delete(String jobId, int version) {
            return false;
        }

        @Override
        public boolean exists(String jobId) {
            return false;
        }

        @Override
        public JDBCJob get(String jobId) {
            return null;
        }

        @Override
        public List<JDBCJob> acquireTriggeredJobs(Date referenceTime, int maxCount) {
            return null;
        }

        @Override
        public void setJobsAsExecuting(Collection<String> jobIds) {

        }
    }

    private static RetryOperations createRetryOperations() {
        Map<Class<? extends Throwable>, Boolean> policyMap = new HashMap<Class<? extends Throwable>, Boolean>();
        policyMap.put(TransientDataAccessException.class, true);
        policyMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(100,  // we really want this to succeed!
                policyMap, true);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(100);
        backOffPolicy.setMultiplier(2);
        backOffPolicy.setMaxInterval(10000);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }
}
