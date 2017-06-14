package com.headstartech.citrine.perf;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.headstartech.burro.BatchingWorkQueue;
import com.headstartech.burro.BatchingWorkQueueConfiguration;
import com.headstartech.burro.MutableBatchingWorkQueueConfiguration;
import com.headstartech.burro.WorkQueue;
import com.headstartech.citrine.Job;
import com.headstartech.citrine.Scheduler;
import com.headstartech.citrine.SchedulerContext;
import com.headstartech.citrine.core.MutableSchedulerConfiguration;
import com.headstartech.citrine.core.SchedulerConfiguration;
import com.headstartech.citrine.core.SchedulerImpl;
import com.headstartech.citrine.jobrunner.ThreadPoolExecutorJobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.jdbc.JDBCJobStore;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAO;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAOImpl;
import com.headstartech.citrine.jobstore.jdbc.dao.std.StdSQLQuerySource;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.DefaultJobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.AsyncJobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.CompletedJobItem;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.CompletedJobItemProcessor;
import com.headstartech.citrine.jobstore.jdbc.jobdata.DefaultJobDataEncoderDecoder;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Per Johansson
 */
public class SchedulerHelper {

    private SchedulerHelper() {}

    public static Scheduler createScheduler(DataSource dataSource, SchedulerContext schedulerContext, int schedulerIndex) {
        SchedulerConfiguration configuration = new MutableSchedulerConfiguration(100, 10000);
        BiMap<Class<? extends Job>, String> mappings = HashBiMap.create();
        mappings.put(TestJob.class, "testjob");
        DefaultJobClassMapper jobClassMapper = new DefaultJobClassMapper(mappings);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(3000));
        return new SchedulerImpl(String.format("scheduler-%d", schedulerIndex),
                configuration,
                createJobStore(dataSource, jobClassMapper),
                new ThreadPoolExecutorJobRunner((ThreadPoolExecutor) executor),
                schedulerContext);
    }

    public static JobStore createJobStore(DataSource dataSource, JobClassMapper jobClassMapper) {
        TransactionOperations transactionOperations = createTransactionOperations(dataSource);
        JobStoreDAO dao = createJobStoreDAO(dataSource);
        return new JDBCJobStore(transactionOperations, dao, jobClassMapper, new DefaultJobDataEncoderDecoder(),
                new AsyncJobCompleter(createWorkQueue(transactionOperations, dao)));
    }

    private static WorkQueue<CompletedJobItem> createWorkQueue(TransactionOperations transactionOperations, JobStoreDAO dao) {
        BatchingWorkQueueConfiguration conf = new MutableBatchingWorkQueueConfiguration("jobCompleter",
                1000,
                1000,
                500,
                500,
                true,
                500);
        WorkQueue<CompletedJobItem> workQueue = new BatchingWorkQueue<>(conf,
                new ArrayBlockingQueue<CompletedJobItem>(5000),
                new CompletedJobItemProcessor(dao, transactionOperations, createRetryOperations()));
        return workQueue;

    }

    public static JobStoreDAO createJobStoreDAO(DataSource dataSource) {
        JobStoreDAO dao = new JobStoreDAOImpl(new NamedParameterJdbcTemplate(dataSource), new StdSQLQuerySource("job"));
        return dao;
    }

    public static TransactionOperations createTransactionOperations(DataSource dataSource) {
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        TransactionOperations transactionOperations = new TransactionTemplate(transactionManager);
        return transactionOperations;
    }

    private static RetryOperations createRetryOperations() {
        Map<Class<? extends Throwable>, Boolean> policyMap = new HashMap<Class<? extends Throwable>, Boolean>();
        policyMap.put(DataAccessException.class, true);
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
