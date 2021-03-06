package com.headstartech.citrine.perf;

import com.headstartech.citrine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Per Johansson
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

    private static Logger logger = LoggerFactory.getLogger(Application.class);

    public static String LATCH_NAME = "latch";
    public static String COUNTER_NAME = "counter";

    private static final AtomicLong jobIdCounter = new AtomicLong();
    private static final AtomicLong jobsExecuted = new AtomicLong();

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        createJobs();

        executeJobs();
    }

    private void executeJobs() throws InterruptedException {
        final int NUMBER_OF_SCHEDULERS = 10;
        final int LATCH_START = 100000;
        CountDownLatch latch = new CountDownLatch(LATCH_START);

        SchedulerContext schedulerContext = new SchedulerContext();
        schedulerContext.put(LATCH_NAME, latch);
        schedulerContext.put(COUNTER_NAME, jobsExecuted);

        List<DataSource> dataSources = MySQLDataSourceFactory.createDataSources(NUMBER_OF_SCHEDULERS);
        List<Scheduler> schedulers = new ArrayList<>();
        for(int i=0; i<NUMBER_OF_SCHEDULERS; ++i) {
            Scheduler scheduler = SchedulerHelper.createScheduler(dataSources.get(i), schedulerContext, i);
            schedulers.add(scheduler);
        }

        long start = 0;
        long end;
        try {
            logger.info("Starting schedulers: count={}", NUMBER_OF_SCHEDULERS);
            for(Scheduler scheduler : schedulers) {
                scheduler.start();
            }

            start = System.currentTimeMillis();
            do {
                logger.info("Waiting for all jobs to have been executed: count = {}", jobsExecuted.get());
            } while(!latch.await(10, TimeUnit.SECONDS));
        } finally {
            ExecutorService executorService = null;
            try {
                logger.info("Stopping schedulers: count={}", NUMBER_OF_SCHEDULERS);
                executorService = Executors.newFixedThreadPool(NUMBER_OF_SCHEDULERS);
                for (final Scheduler scheduler : schedulers) {
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            scheduler.shutdown(true);
                        }
                    });
                }
            } finally {
                executorService.shutdown();
                executorService.awaitTermination(120, TimeUnit.SECONDS);
            }
            end = System.currentTimeMillis();
            logger.info("Rate {} jobs/s, jobs {}", (jobsExecuted.get()/ ((end - start)/ 1000)), jobsExecuted.get());

            for(DataSource ds : dataSources) {
                MySQLDataSourceFactory.close(ds);
            }
        }
    }

    private void createJobs() {
        final int NUMBER_OF_JOBS = 1000000;

        DataSource dataSource = null;
        Scheduler scheduler = null;

        try {
            dataSource = MySQLDataSourceFactory.createDataSource();
            scheduler = SchedulerHelper.createScheduler(dataSource, new SchedulerContext(), 0);

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
            final Date now = new Date();
            final int bound = 5000;
            final int signThreshold = 0;
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
                            int sign = rng.nextInt(100) >= signThreshold ? -1 : 1;
                            Date scheduleAt = new Date(now.getTime() + sign * 1000000 * rng.nextInt(bound));
                            JobDetail jd = new JobDetail(jobKey, TestJob.class, new SimpleTrigger(scheduleAt), new JobData(), (short) rng.nextInt(3));
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
