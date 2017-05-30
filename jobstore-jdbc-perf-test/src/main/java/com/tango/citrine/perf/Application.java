package com.tango.citrine.perf;

import com.tango.citrine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Per Johansson
 */
@SpringBootApplication
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class Application implements CommandLineRunner {

    private static Logger logger = LoggerFactory.getLogger(Application.class);

    private static final AtomicLong jobIdCounter = new AtomicLong();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        final int NUMBER_OF_JOBS = 500000;

        DataSource dataSource = null;
        Scheduler scheduler = null;

        try {
            dataSource = MySQLDataSourceFactory.createDataSource();
            scheduler = SchedulerHelper.createScheduler(dataSource);

            logger.info("Creating jobs: count={}", NUMBER_OF_JOBS);
            long start = System.currentTimeMillis();
            createJobs(scheduler, NUMBER_OF_JOBS);
            logger.info("Created jobs: count={}, elapsedTime = {} secs", NUMBER_OF_JOBS, (System.currentTimeMillis() - start) / 1000);
        } finally {
            if(scheduler != null) {
                scheduler.shutdown();
            }
            if(dataSource != null) {
                MySQLDataSourceFactory.close(dataSource);
            }
        }
    }

    private void createJobs(final Scheduler scheduler, final int numberOfJobs) {
        DataSource dataSource = null;
        try {
            dataSource = MySQLDataSourceFactory.createDataSource();
            TransactionOperations transactionOperations = SchedulerHelper.createTransactionOperations(dataSource);
            final Random rng = new Random(System.currentTimeMillis());
            final int batchSize = Math.min(50000, numberOfJobs);
            final AtomicLong totalScheduled = new AtomicLong();
            for (int i = 0; i < numberOfJobs / batchSize; ++i) {
                transactionOperations.execute(new TransactionCallback<Void>() {
                    @Override
                    public Void doInTransaction(TransactionStatus status) {
                        List<JobDetail> jobs = new ArrayList<JobDetail>(batchSize);
                        for (int j = 0; j < batchSize; ++j) {
                            JobKey jobKey = new JobKey(String.valueOf(System.currentTimeMillis()) + "-" + jobIdCounter.incrementAndGet());
                            Date d = new Date();
                            JobDetail jd = new JobDetail(jobKey, TestJob.class, new SimpleTrigger(d), new JobData(), (short) rng.nextInt(3));
                            jobs.add(jd);
                        }
                        try {
                            scheduler.scheduleJobs(jobs);
                            totalScheduled.addAndGet(batchSize);
                            logger.info("Scheduled {} jobs (out of {})...", totalScheduled.get(), numberOfJobs);
                        } catch (JobAlreadyExistsException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                });
            }
        } finally {
            if(dataSource != null) {
                MySQLDataSourceFactory.close(dataSource);
            }
        }
    }
}
