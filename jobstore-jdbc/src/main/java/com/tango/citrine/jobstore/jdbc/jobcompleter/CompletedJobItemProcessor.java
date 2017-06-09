package com.tango.citrine.jobstore.jdbc.jobcompleter;

import com.google.common.base.Optional;
import com.headstartech.burro.WorkProcessor;
import com.tango.citrine.DefaultTriggerContext;
import com.tango.citrine.JobDetail;
import com.tango.citrine.JobKey;
import com.tango.citrine.jobstore.JobState;
import com.tango.citrine.jobstore.TriggeredJobCompleteAction;
import com.tango.citrine.jobstore.VersionedTriggeredJob;
import com.tango.citrine.jobstore.jdbc.dao.JDBCJob;
import com.tango.citrine.jobstore.jdbc.dao.JobStoreDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

import java.util.*;

/**
 * @author Per Johansson
 */
public class CompletedJobItemProcessor implements WorkProcessor<Collection<CompletedJobItem>> {

    private static final Logger logger = LoggerFactory.getLogger(CompletedJobItemProcessor.class);

    private final JobStoreDAO dao;
    private final TransactionOperations transactionOperations;
    private final RetryOperations retryOperations;

    public CompletedJobItemProcessor(JobStoreDAO dao, TransactionOperations transactionOperations, RetryOperations retryOperations) {
        this.dao = dao;
        this.transactionOperations = transactionOperations;
        this.retryOperations = retryOperations;
    }

    @Override
    public void process(final Collection<CompletedJobItem> items) {
        logger.debug("Processing {} items...", items.size());
        final List<CompletedJobItem> sorted = new ArrayList<CompletedJobItem>(items.size());
        sorted.addAll(items);
        Collections.sort(sorted, new Comparator<CompletedJobItem>() {
                    @Override
                    public int compare(CompletedJobItem o1, CompletedJobItem o2) {
                        return o1.getTriggeredJob().getJobDetail().getJobKey().getKey().compareTo(o2.getTriggeredJob().getJobDetail().getJobKey().getKey());
                    }
                }
        );
        retryOperations.execute(new RetryCallback<Object, RuntimeException>() {
            @Override
            public Object doWithRetry(RetryContext context) throws RuntimeException {
                transactionOperations.execute(new TransactionCallback<Void>() {
                    @Override
                    public Void doInTransaction(TransactionStatus status) {
                        for(CompletedJobItem item : sorted) {
                            doCompleteJob(item.getTriggeredJob(), item.getAction());
                        }
                        return null;
                    }
                });
                return null;
            }
        });
        logger.debug("Processing completed");
    }

    protected void doCompleteJob(VersionedTriggeredJob triggeredJob, TriggeredJobCompleteAction action) {
        JobDetail jobDetail = triggeredJob.getJobDetail();
        JobKey jobKey = jobDetail.getJobKey();

        if (TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB.equals(action)) {
            Optional<Date> nextExecutionTimeOpt = jobDetail.getTrigger().nextExecutionTime(new DefaultTriggerContext(triggeredJob.scheduledExecutionTime(), triggeredJob.actualExecutionTime()));
            if(nextExecutionTimeOpt.isPresent()) {
                JDBCJob job = dao.get(jobKey.getKey());
                if (job == null) {
                    return;
                }
                job.setNextExecutionTime(nextExecutionTimeOpt.get());
                job.setJobState(JobState.WAITING);
                job.setVersion(triggeredJob.getExpectedVersionIfNoUpdate());
                boolean res = dao.updateJob(job);
                if(!res) {
                    logger.debug("Job has been updated concurrently, action not executed: jobKey={}, action={}", jobKey, action);
                }
            } else {
                deleteJob(triggeredJob);
            }
        } else if (TriggeredJobCompleteAction.SET_JOB_WAITING.equals(action)) {
            JDBCJob job = dao.get(jobKey.getKey());
            if (job == null) {
                return;
            }
            job.setJobState(JobState.WAITING);
            job.setVersion(triggeredJob.getExpectedVersionIfNoUpdate());
            boolean res = dao.updateJob(job);
            if(!res) {
                logger.debug("Job has been updated concurrently, action not executed: jobKey={}, action={}", jobKey, action);
            }
        } else if (TriggeredJobCompleteAction.SET_JOB_ERROR.equals(action)) {
            JDBCJob job = dao.get(jobKey.getKey());
            if (job == null) {
                return;
            }
            job.setJobState(JobState.ERROR);
            job.setVersion(triggeredJob.getExpectedVersionIfNoUpdate());
            boolean res = dao.updateJob(job);
            if(!res) {
                logger.debug("Job has been updated concurrently, action not executed: jobKey={}, action={}", jobKey, action);
            }
        }
    }

    protected void deleteJob(VersionedTriggeredJob versionedTriggeredJob) {
        JobDetail jobDetail = versionedTriggeredJob.getJobDetail();
        JobKey jobKey = jobDetail.getJobKey();
        boolean res = dao.delete(jobKey.getKey(), versionedTriggeredJob.getExpectedVersionIfNoUpdate());
        if(!res) {
            logger.debug("Job has been updated concurrently, action not executed: jobKey={}, action={}", jobKey, TriggeredJobCompleteAction.DELETE_OR_RESCHEDULE_JOB);
        }
    }
}
