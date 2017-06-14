package com.headstartech.citrine.jobstore.jdbc.jobcompleter;

import com.headstartech.citrine.JobData;
import com.headstartech.citrine.JobDetail;
import com.headstartech.citrine.JobKey;
import com.headstartech.citrine.SimpleTrigger;
import com.headstartech.citrine.jobstore.TestJobClass1;
import com.headstartech.citrine.jobstore.TriggeredJobCompleteAction;
import com.headstartech.citrine.jobstore.VersionedTriggeredJob;
import com.headstartech.citrine.jobstore.jdbc.TestUtils;
import org.junit.Test;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Date;

import static org.junit.Assert.assertFalse;

/**
 * Created by per on 13/01/16.
 */
public class DefaultJobCompleterTest {

    @Test
    public void shutdown() throws InterruptedException {
        // given
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new AlwaysRetryPolicy());
        final JobCompleter jobCompleter = new DefaultJobCompleter(new TestUtils.DummyDAO(), new TestUtils.ExceptionThrowingTransactionOperations(new RuntimeException()), retryTemplate);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                JobDetail jd = new JobDetail(new JobKey("foo"), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData());
                VersionedTriggeredJob versionedTriggeredJob = new VersionedTriggeredJob(jd, 0, new Date(), new Date());
                // jobCompleter should retry infinite amount of times
                jobCompleter.completeJob(versionedTriggeredJob, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
            }
        });
        t.start();

        // when
        jobCompleter.shutdown();

        // then
        t.join(1000);
        assertFalse(t.isAlive());
    }
}
