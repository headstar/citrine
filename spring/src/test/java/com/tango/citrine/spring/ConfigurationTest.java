package com.tango.citrine.spring;

import com.tango.citrine.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by per on 14/01/16.
 */

public class ConfigurationTest {

    private static SchedulerContext schedulerContext = new SchedulerContext();
    private static JobListener jobListener = new TestJobListener();

    @Test
    public void jdbcConfiguration() {
        runWithContext(JDBCConfiguration.class, new TestCallback() {
            @Override
            public void testCall(ApplicationContext applicationContext) {
                applicationContext.getBean("citrineScheduler", Scheduler.class);

            }
        });
    }

    @Test(expected = org.springframework.beans.factory.BeanCreationException.class)
    public void jdbcConfigurationWithNonExistingDatasource() {
       new AnnotationConfigApplicationContext(JDBConfigurationWithNonExistingDataSource.class);
    }

    @Test(expected = org.springframework.beans.factory.BeanCreationException.class)
    public void jdbcConfigurationWithNonExistingTransactionOperations() {
        new AnnotationConfigApplicationContext(JDBConfigurationWithNonExistingTransactionOperations.class);
    }

    private static void runWithContext(Class<?> clazz, TestCallback callback) {
        ApplicationContext ctx = null;
        try {
            ctx = new AnnotationConfigApplicationContext(clazz);
            callback.testCall(ctx);
        } finally {
            if(ctx != null) {
                ((ConfigurableApplicationContext) ctx).close();
            }
        }
    }

    interface TestCallback {
        void testCall(ApplicationContext applicationContext);
    }

    static class Base {

        /**
         * To enable evaluation of @Value annotations
         * @return
         */
        @Bean
        public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
            return new PropertySourcesPlaceholderConfigurer();
        }

    }

    static class JobClass implements Job {

        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            // do nothing
        }
    }

    static class TestJobListener implements JobListener {
        @Override
        public void jobToBeExecuted(JobExecutionContext context) {

        }
    }

    @EnableCitrine(jobStoreType = JobStoreType.JDBC, dataSource = "aDataSource", transactionManager = "aTransactionMaanager")
    @Configuration
    static class JDBCConfiguration extends Base {

        private static final String H2_JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
        private static final String H2_USER = "sa";
        private static final String H2_PASSWORD = "";

        @Bean
        public DataSource aDataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL(H2_JDBC_URL);
            dataSource.setUser(H2_USER);
            dataSource.setPassword(H2_PASSWORD);
            return dataSource;
        }

        @Bean
        public PlatformTransactionManager aTransactionMaanager() {
            return new DataSourceTransactionManager(aDataSource());
        }

    }

    @EnableCitrine(jobStoreType = JobStoreType.JDBC, dataSource = "notADataSource", transactionManager = "aTransactionOperations")
    @Configuration
    static class JDBConfigurationWithNonExistingDataSource extends Base {

        private static final String H2_JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
        private static final String H2_USER = "sa";
        private static final String H2_PASSWORD = "";

        @Bean
        public DataSource aDataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL(H2_JDBC_URL);
            dataSource.setUser(H2_USER);
            dataSource.setPassword(H2_PASSWORD);
            return dataSource;
        }

        @Bean
        public TransactionOperations aTransactionOperations() {
            PlatformTransactionManager transactionManager = new DataSourceTransactionManager(aDataSource());
            return new TransactionTemplate(transactionManager);
        }
    }

    @EnableCitrine(jobStoreType = JobStoreType.JDBC, dataSource = "dataSource", transactionManager = "notATransactionOperations")
    @Configuration
    static class JDBConfigurationWithNonExistingTransactionOperations extends Base {

        private static final String H2_JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
        private static final String H2_USER = "sa";
        private static final String H2_PASSWORD = "";

        @Bean
        public DataSource aDataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL(H2_JDBC_URL);
            dataSource.setUser(H2_USER);
            dataSource.setPassword(H2_PASSWORD);
            return dataSource;
        }

        @Bean
        public TransactionOperations aTransactionOperations() {
            PlatformTransactionManager transactionManager = new DataSourceTransactionManager(aDataSource());
            return new TransactionTemplate(transactionManager);
        }
    }

    @EnableCitrine
    @Configuration
    static class BasicConfiguration extends Base {

        @Bean
        public JobDetail job1() {
            return new JobDetail(new JobKey("job1"), JobClass.class, new SimpleTrigger(new Date(System.currentTimeMillis() + 10000)), new JobData());
        }

        @Bean
        public JobDetail job2() {
            return new JobDetail(new JobKey("job2"), JobClass.class, new SimpleTrigger(new Date(System.currentTimeMillis() + 10000)), new JobData());
        }

        @Bean
        public SchedulerContext schedulerContext() {
            return schedulerContext;
        }

        @Bean
        public JobListener jobListener() {
            return jobListener;
        }
    }

    private static Properties loadProperties() {
        try {
            return  PropertiesLoaderUtils.loadAllProperties("jdbc.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
