package com.tango.citrine.spring;

import com.tango.citrine.*;
import com.tango.citrine.core.SchedulerConfiguration;
import com.tango.citrine.core.SchedulerImpl;
import com.tango.citrine.jobrunner.JobRunner;
import com.tango.citrine.jobstore.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by per on 30/03/15.
 */
public class SchedulerFactoryBean implements FactoryBean<Scheduler>, SmartLifecycle, BeanNameAware {

    private final Logger logger = LoggerFactory.getLogger(SchedulerFactoryBean.class);

    private Scheduler scheduler;
    private int phase = Integer.MAX_VALUE;
    private JobStore jobStore;
    private JobRunner jobRunner;
    private SchedulerContext schedulerContext;
    private boolean autoStartup = true;
    private boolean waitForJobsToCompleteOnShutdown = true;
    private boolean running = false;
    private boolean overwriteExistingJobs = false;
    private List<JobDetail> jobs;
    private List<JobListener> jobListeners;
    private SchedulerConfiguration schedulerConfiguration;
    private String schedulerName;
    private PlatformTransactionManager transactionManager;
    private ThreadPoolTaskExecutor taskExecutor;

    private static final ThreadLocal<ThreadPoolTaskExecutor> configurationTimeTaskExecutorHolder = new ThreadLocal<ThreadPoolTaskExecutor>();

    @PostConstruct
    public void initialize() {
        if(schedulerContext == null) {
            schedulerContext = new SchedulerContext();
        }

        scheduler = new SchedulerImpl(schedulerName, schedulerConfiguration, jobStore, jobRunner, schedulerContext);

        try {
            configurationTimeTaskExecutorHolder.set(taskExecutor);
            scheduler.initialize();
        } finally {
            configurationTimeTaskExecutorHolder.remove();
        }

        if(jobListeners != null) {
            for(JobListener el : jobListeners) {
                scheduler.getListenerRegistry().addJobListener(el);
            }
        }

        if(jobs != null && !jobs.isEmpty()) {
            if(transactionManager == null) {
                addJobs();
            } else {
                TransactionOperations transactionOperations = new TransactionTemplate(transactionManager);
                transactionOperations.execute(new TransactionCallback<Void>() {
                    @Override
                    public Void doInTransaction(TransactionStatus status) {
                        addJobs();
                        return null;
                    }
                });
            }
        }
    }

    private void addJobs() {
        for (JobDetail jd : jobs) {
            try {
                if(overwriteExistingJobs || !scheduler.exists(jd.getJobKey())) {
                    scheduler.scheduleJob(jd, true);
                }
            } catch (SchedulerException e) {
                throw new RuntimeException("failed to add job", e);
            }
        }
    }

    @Override
    public Scheduler getObject() throws Exception {
        return scheduler;
    }

    @Override
    public Class<? extends Scheduler> getObjectType() {
        return (this.scheduler != null) ? this.scheduler.getClass() : Scheduler.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        if(scheduler != null) {
            try {
                scheduler.start();
                running = true;
            } catch (SchedulerException e) {
                throw new RuntimeException("Failed to start job scheduler", e);
            }
        }
    }

    @Override
    public void stop() {
        if(scheduler != null) {
            scheduler.shutdown(waitForJobsToCompleteOnShutdown);
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    public void setPhase(int phase) {
        this.phase = phase;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    @Override
    public void setBeanName(String name) {
        if (this.schedulerName == null) {
            this.schedulerName = name;
        }
    }

    public void setJobStore(JobStore jobStore) {
        this.jobStore = jobStore;
    }

    public void setJobRunner(JobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    public void setSchedulerContext(SchedulerContext schedulerContext) {
        this.schedulerContext = schedulerContext;
    }

    public void setWaitForJobsToCompleteOnShutdown(boolean waitForJobsToCompleteOnShutdown) {
        this.waitForJobsToCompleteOnShutdown = waitForJobsToCompleteOnShutdown;
    }

    public void setJobs(List<JobDetail> jobs) {
        this.jobs = jobs;
    }

    public void setSchedulerConfiguration(SchedulerConfiguration schedulerConfiguration) {
        this.schedulerConfiguration = schedulerConfiguration;
    }

    public void setOverwriteExistingJobs(boolean overwriteExistingJobs) {
        this.overwriteExistingJobs = overwriteExistingJobs;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setJobListeners(List<JobListener> jobListeners) {
        this.jobListeners = jobListeners;
    }

    public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public static ThreadPoolTaskExecutor getConfigurationTimeTaskExecutorHolder() {
        return configurationTimeTaskExecutorHolder.get();
    }
}
