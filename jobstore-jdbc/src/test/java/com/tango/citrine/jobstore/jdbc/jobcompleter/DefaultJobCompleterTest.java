package com.tango.citrine.jobstore.jdbc.jobcompleter;

import com.tango.citrine.JobData;
import com.tango.citrine.JobDetail;
import com.tango.citrine.JobKey;
import com.tango.citrine.SimpleTrigger;
import com.tango.citrine.jobstore.TestJobClass1;
import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;
import com.tango.citrine.jobstore.jdbc.TestUtils;
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
