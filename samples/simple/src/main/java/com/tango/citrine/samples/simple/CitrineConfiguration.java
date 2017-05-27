package com.tango.citrine.samples.simple;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.tango.citrine.*;
import com.tango.citrine.jobstore.jdbc.jobclassmapper.DefaultJobClassMapper;
import com.tango.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.tango.citrine.spring.EnableCitrine;
import com.tango.citrine.spring.JobStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;

@Configuration
@EnableCitrine
public class CitrineConfiguration {

    @Autowired
    private ExampleService fooService;

    /**
     * Creating a simple job
     * @return
     */
    @Bean
    public JobDetail job1() {
        return new JobDetail(new JobKey("simpleJob1"), SimpleJobClass.class, new SimpleTrigger(new Date(System.currentTimeMillis() + 10000)), new JobData());
    }

    /**
     * Creating a cron job
     * @return
     */
    @Bean
    public JobDetail job2() {
        return new JobDetail(new JobKey("cronJob1"), CronJobClass.class, new CronTrigger("0/15 * * * * ?"), new JobData());
    }

    /**
     * Creating a context accessible when executing jobs.
     *
     * @return
     */
    @Bean
    public SchedulerContext schedulerContext() {
        SchedulerContext ctx = new SchedulerContext();
        ctx.put("fooService", fooService);  // service to be used when executing a job
        return ctx;
    }

    /**
     * Creating a simple job listener, logging job data.
     *
     * @return
     */
    @Bean
    public JobListener jobListener() {
        return new LoggingJobListener();
    }

    /**
     * Creating a JobClassMapper to use a specified name instead of the fully qualified class name when persisting the job. Saves some disk space and makes it easier to
     * refactor later.
     *
     * @return
     */
    @Bean
    public JobClassMapper jobClassMapper() {
        BiMap<Class<? extends Job>, String> mappings = HashBiMap.create();
        mappings.put(SimpleJobClass.class, "simpleJob");
        return new DefaultJobClassMapper(mappings);
    }

    public static class SimpleJobClass implements Job {

        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            ExampleService fooService = (ExampleService) jobExecutionContext.getScheduler().getContext().get("fooService");
            fooService.log("Executing simple job!");
        }
    }

    public static class CronJobClass implements Job {

        @Override
        public void execute(JobExecutionContext jobExecutionContext) {
            ExampleService fooService = (ExampleService) jobExecutionContext.getScheduler().getContext().get("fooService");
            fooService.log("Executing cron job!");
        }
    }

    public static class LoggingJobListener implements JobListener {

        private static Logger logger = LoggerFactory.getLogger(LoggingJobListener.class);

        @Override
        public void jobToBeExecuted(JobExecutionContext context) {
            logger.info("Job listener called, job to be executed: jobDetail={}", context.getTriggeredJob().getJobDetail());
        }
    }

}
