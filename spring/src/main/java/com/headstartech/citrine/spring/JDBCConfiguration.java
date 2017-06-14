package com.headstartech.citrine.spring;

import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.jdbc.JDBCJobStore;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAO;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAOImpl;
import com.headstartech.citrine.jobstore.jdbc.dao.QuerySource;
import com.headstartech.citrine.jobstore.jdbc.dao.std.StdSQLQuerySource;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.DefaultJobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.DefaultJobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.JobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobdata.DefaultJobDataEncoderDecoder;
import com.headstartech.citrine.jobstore.jdbc.jobdata.JobDataEncoderDecoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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

/**
 * Created by per on 12/01/16.
 */
@Configuration
public class JDBCConfiguration extends AbstractJobStoreConfiguration {

    private static final String DATABASE_TYPE_ATTRIBUTE_NAME = "databaseType";
    private static final String TABLE_NAME_ATTRIBUTE_NAME = "tableName";
    private static final String DATASOURCE_ATTRIBUTE_NAME = "dataSource";
    private static final String TRANSATION_MANAGER_ATTRIBUTE_NAME = "transactionManager";

    @Autowired(required = false)
    private JobClassMapper jobClassMapper;

    @Autowired(required = false)
    private JobDataEncoderDecoder jobDataEncoderDecoder;

    @Autowired(required = false)
    private JobCompleter jobCompleter;

    @Bean
    public JobStore jobStore() {
        DatabaseType databaseType = enableCitrine.getEnum(DATABASE_TYPE_ATTRIBUTE_NAME);
        String tableName = enableCitrine.getString(TABLE_NAME_ATTRIBUTE_NAME);

        QuerySource querySource = getQuerySource(databaseType, tableName);
        if (querySource == null) {
            throw new IllegalArgumentException(String.format("Unknown JDBCType: '%s'", databaseType));
        }

        DataSource dataSource = getRequiredBeanFromAttribute(DATASOURCE_ATTRIBUTE_NAME, DataSource.class);
        JobStoreDAO dao = new JobStoreDAOImpl(new NamedParameterJdbcTemplate(dataSource), querySource);

        if(jobClassMapper == null) {
            jobClassMapper = new DefaultJobClassMapper();
        }

        if(jobDataEncoderDecoder == null) {
            jobDataEncoderDecoder = new DefaultJobDataEncoderDecoder();
        }

        PlatformTransactionManager transactionManager = getRequiredBeanFromAttribute(TRANSATION_MANAGER_ATTRIBUTE_NAME, PlatformTransactionManager.class);

        TransactionOperations transactionOperations = new TransactionTemplate(transactionManager);
        if(jobCompleter == null) {
            jobCompleter = new DefaultJobCompleter(dao, transactionOperations, createRetryOperations());
        }

        return new JDBCJobStore(transactionOperations, dao, jobClassMapper, jobDataEncoderDecoder, jobCompleter);
    }

    @Bean
    public TransactionManagerHolder transactionManagerHolder() {
        return new TransactionManagerHolder(getRequiredBeanFromAttribute(TRANSATION_MANAGER_ATTRIBUTE_NAME, PlatformTransactionManager.class));
    }

    private QuerySource getQuerySource(DatabaseType databaseType, String tableName) {
        switch (databaseType) {
            case MYSQL:
                return new StdSQLQuerySource(tableName);
            default:
                return null;
        }
    }

    private RetryOperations createRetryOperations() {
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
