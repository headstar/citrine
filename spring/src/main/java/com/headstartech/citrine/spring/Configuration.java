package com.headstartech.citrine.spring;

import com.headstartech.citrine.JobDetail;
import com.headstartech.citrine.JobListener;
import com.headstartech.citrine.SchedulerContext;
import com.headstartech.citrine.core.MutableSchedulerConfiguration;
import com.headstartech.citrine.core.SchedulerConfiguration;
import com.headstartech.citrine.jobrunner.JobRunner;
import com.headstartech.citrine.jobstore.JobStore;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;

/**
 * Created by per on 13/01/16.
 */
@org.springframework.context.annotation.Configuration
public class Configuration implements BeanFactoryAware {

    private final String citrineDummyBean = "citrineInitializationDummy";

    @Value("${citrine.overwrite-existing-jobs:false}")
    private boolean overwriteExistingJobs;

    @Value("${citrine.auto-startup:true}")
    private boolean autoStartup;

    @Value("${citrine.scheduler.wait-for-jobs-to-complete-on-shutdown:true}")
    private boolean waitForJobsToCompleteOnShutdown;

    @Value("${citrine.scheduler.name:citrineScheduler}")
    private String schedulerName;

    @Value("${citrine.scheduler.idle-wait-time:1000}")
    private int idleWaitTime;

    @Value("${citrine.scheduler.max-batch-size:10000}")
    private int maxBatchSize;

    @Value("${citrine.executor.core-pool-size:1}")
    private int executorCorePoolSize;

    @Value("${citrine.executor.max-pool-size:1}")
    private int executorMaxPoolSize;

    @Value("${citrine.executor.queue-capacity:0}")
    private int executorQueueCapacity;

    @Value("${citrine.executor.wait-for-tasks-to-complete-on-shutdown:true}")
    private boolean executorWaitForTasksToCompleteOnShutdown;

    @Value("${citrine.jobrunnner.min-runnables-acccepted-threshold:0}")
    private int jobRunnerMinRunnablesAcceptedThreshold;

    @Value("${citrine.scheduler.intialize-after-bean:}")
    private String intializeAfter;

    @Autowired
    private JobStore jobStore;

    @Autowired(required = false)
    private JobRunner jobRunner;

    @Autowired(required = false)
    private SchedulerContext schedulerContext;

    @Autowired(required = false)
    private List<JobListener> jobListeners;

    @Autowired(required = false)
    private List<JobDetail> jobs;

    @Autowired(required = false)
    private TransactionManagerHolder transactionManagerHolder;

    @Autowired(required = false)
    @Qualifier("citrineTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private BeanFactory beanFactory;

    /**
     * Create dummy bean to be able to force the scheduler bean to be initialized after a specified bean (e.g. "flywayInitializer").
     *
     * @return
     */
    @Bean(name = citrineDummyBean)
    public Object citrineInitializationDummy() {
        if(!intializeAfter.isEmpty()) {
            beanFactory.getBean(intializeAfter);
        }
        return new Object();
    }

    @DependsOn(citrineDummyBean)
    @Bean(name = "citrineScheduler")
    public SchedulerFactoryBean schedulerFactoryBean() {
        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setAutoStartup(autoStartup);
        factory.setOverwriteExistingJobs(overwriteExistingJobs);
        factory.setWaitForJobsToCompleteOnShutdown(waitForJobsToCompleteOnShutdown);
        factory.setJobStore(jobStore);
        factory.setJobRunner(jobRunner);
        factory.setSchedulerContext(schedulerContext);
        factory.setJobListeners(jobListeners);
        factory.setJobs(jobs);
        factory.setSchedulerConfiguration(schedulerConfiguration());
        factory.setSchedulerName(schedulerName);

        if(jobRunner == null) {
            ThreadPoolTaskExecutorJobRunner jobRunner = new ThreadPoolTaskExecutorJobRunner();
            jobRunner.setMinAcceptedThreshold(jobRunnerMinRunnablesAcceptedThreshold);
            factory.setJobRunner(jobRunner);
        }

        if(taskExecutor == null) {
            factory.setTaskExecutor(defaultCitrineTaskExecutor());
        } else {
            factory.setTaskExecutor(taskExecutor);
        }

        if(transactionManagerHolder != null) {
            factory.setTransactionManager(transactionManagerHolder.getTransactionManager());
        }

        return factory;
    }

    @Bean
    public ThreadPoolTaskExecutor defaultCitrineTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadFactory(new CustomizableThreadFactory("citrine-job-runner"));
        executor.setCorePoolSize(executorCorePoolSize);
        executor.setMaxPoolSize(executorMaxPoolSize);
        executor.setQueueCapacity(executorQueueCapacity);
        executor.setWaitForTasksToCompleteOnShutdown(executorWaitForTasksToCompleteOnShutdown);
        return executor;
    }

    @Bean
    public SchedulerConfiguration schedulerConfiguration() {
        return new MutableSchedulerConfiguration(idleWaitTime, maxBatchSize);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

}
