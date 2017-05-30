package com.tango.citrine.perf;

import com.tango.citrine.Job;
import com.tango.citrine.JobExecutionContext;

import java.util.concurrent.CountDownLatch;

import static com.tango.citrine.perf.Application.LATCH_NAME;

/**
 * @author Per Johansson
 */
public class TestJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        CountDownLatch latch = (CountDownLatch) jobExecutionContext.getScheduler().getContext().get(LATCH_NAME);
        latch.countDown();
    }
}
