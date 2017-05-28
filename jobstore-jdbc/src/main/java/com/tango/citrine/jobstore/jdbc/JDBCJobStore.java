package com.tango.citrine.jobstore.jdbc;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.tango.citrine.*;
import com.tango.citrine.jobstore.*;
import com.tango.citrine.jobstore.jdbc.dao.JDBCJob;
import com.tango.citrine.jobstore.jdbc.dao.JobStoreDAO;
import com.tango.citrine.jobstore.jdbc.jobclassmapper.JobClassMapper;
import com.tango.citrine.jobstore.jdbc.jobcompleter.JobCompleter;
import com.tango.citrine.jobstore.jdbc.jobdata.JobDataEncoderDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by per on 02/12/15.
 */
public class JDBCJobStore implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(JDBCJobStore.class);

    private final TransactionOperations transactionOperations;
    private final JobStoreDAO dao;
    private final JobClassMapper jobClassMapper;
    private final JobDataEncoderDecoder jobDataEncoderDecoder;
    private final JobCompleter jobCompleter;

    public JDBCJobStore(TransactionOperations transactionOperations, JobStoreDAO dao, JobClassMapper jobClassMapper,
                        JobDataEncoderDecoder jobDataEncoderDecoder, JobCompleter jobCompleter) {
        Preconditions.checkNotNull(transactionOperations);
        Preconditions.checkNotNull(dao);
        Preconditions.checkNotNull(jobClassMapper);
        Preconditions.checkNotNull(jobDataEncoderDecoder);
        Preconditions.checkNotNull(jobCompleter);
        this.transactionOperations = transactionOperations;
        this.dao = dao;
        this.jobClassMapper = jobClassMapper;
        this.jobDataEncoderDecoder = jobDataEncoderDecoder;
        this.jobCompleter = jobCompleter;
    }

    @Override
    public void initialize() {
        // do nothing
    }

    @Override
    public boolean addJob(final JobDetail jobDetail, final boolean replace) throws JobAlreadyExistsException {
        try {
            return executeInTransaction(new TransactionCallback<Boolean>() {
                @Override
                public Boolean doInTransaction(TransactionStatus status) {
                    boolean replaced = false;
                    try {
                        dao.insert(createJDBCJob(jobDetail));
                    } catch(DuplicateKeyException e) {
                        if(!replace) {
                            throw new NestedException(new JobAlreadyExistsException(String.format("Job already exists: jobKey=%s", jobDetail.getJobKey())));
                        } else {
                            JDBCJob existingJob = dao.get(jobDetail.getJobKey().getKey());
                            JDBCJob replacingJob = createJDBCJob(jobDetail);
                            replacingJob.setVersion(existingJob.getVersion());
                            dao.updateJob(replacingJob);
                            replaced = true;
                        }
                    }
                    return replaced;
                }
            });
        } catch (NestedException e) {
            throw (JobAlreadyExistsException) e.getCause();
        }
    }

    @Override
    public void addJobs(final Iterable<JobDetail> jobDetails) throws JobAlreadyExistsException {
        try {
            executeInTransaction(new TransactionCallback<Void>() {
                @Override
                public Void doInTransaction(TransactionStatus status) {
                    try {
                        dao.insert(createJDBCJobs(jobDetails));
                    } catch(DuplicateKeyException e) {
                        throw new NestedException(new JobAlreadyExistsException(String.format("Job already exists")));
                    }
                    return null;
                }
            });
        } catch (NestedException e) {
            throw (JobAlreadyExistsException) e.getCause();
        }
    }

    @Override
    public boolean removeJob(final JobKey jobKey) {
        return executeInTransaction(new TransactionCallback<Boolean>() {
            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                return dao.delete(jobKey.getKey());
            }
        });
    }

    @Override
    public boolean exists(final JobKey jobKey) {
        return executeInTransaction(new TransactionCallback<Boolean>() {
            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                return dao.exists(jobKey.getKey());
            }
        });
    }

    @Override
    public Optional<JobDetail> getJob(final JobKey jobKey) throws SchedulerException {
        try {
            return executeInTransaction(new TransactionCallback<Optional<JobDetail>>() {
                @Override
                public Optional<JobDetail> doInTransaction(TransactionStatus status) {
                    JDBCJob jdbcJob = dao.get(jobKey.getKey());
                    if (jdbcJob == null) {
                        return Optional.absent();
                    } else {
                        try {
                            return Optional.of(convertFromJDBCJob(jdbcJob));
                        } catch (ClassNotFoundException e) {
                            throw new NestedException(e);
                        }
                    }
                }
            });
        } catch(NestedException e) {
            throw new SchedulerException(e.getCause());
        }
    }

    @Override
    public Optional<JobState> getJobState(final JobKey jobKey) {
        return executeInTransaction(new TransactionCallback<Optional<JobState>>() {
            @Override
            public Optional<JobState> doInTransaction(TransactionStatus status) {
                JDBCJob jdbcJob = dao.get(jobKey.getKey());
                if(jdbcJob != null) {
                    return Optional.of(jdbcJob.getJobState());
                } else {
                    return Optional.absent();
                }
            }
        });
    }

    @Override
    public List<TriggeredJob> acquireTriggeredJobs(final Date currentTime, final int maxCount) throws SchedulerException {
        try {
            return executeInTransaction(new TransactionCallback<List<TriggeredJob>>() {
                @Override
                public List<TriggeredJob> doInTransaction(TransactionStatus status) {
                    List<TriggeredJob> res = new ArrayList<TriggeredJob>();
                    if (maxCount > 0) {
                        List<JDBCJob> acquired = dao.acquireTriggeredJobs(currentTime, maxCount);
                        Date acquiredTime = new Date();
                        Collection<String> jobsToSetAsExecuting = new ArrayList<String>(acquired.size());
                        for (JDBCJob jdbcJob : acquired) {
                            try {
                                int expectedJobVersionIfNoUpdate = jdbcJob.getVersion() + 1;
                                res.add(new VersionedTriggeredJob(convertFromJDBCJob(jdbcJob), expectedJobVersionIfNoUpdate, jdbcJob.getNextExecutionTime(), acquiredTime));
                                jobsToSetAsExecuting.add(jdbcJob.getId());
                            } catch (ClassNotFoundException e) {
                                logger.warn(String.format("Failed to create JobDetail when acquiring job: jobKey=%s", jdbcJob.getId()), e);
                                jdbcJob.setJobState(JobState.ERROR);
                                dao.updateJob(jdbcJob);
                            }
                        }
                        dao.setJobsAsExecuting(jobsToSetAsExecuting);
                    }
                    return res;
                }
            });
        } catch(JobPersistenceException e) {
            return new ArrayList<TriggeredJob>();
        }
    }

    @Override
    public void triggeredJobComplete(TriggeredJob triggeredJob, TriggeredJobCompleteAction action) {
        if (!(triggeredJob instanceof VersionedTriggeredJob)) {
            logger.error("TriggeredJob not instance of expected class: expected={}, actual={}", VersionedTriggeredJob.class.getName(), triggeredJob.getClass().getName());
            return;
        }
        try {
            jobCompleter.completeJob((VersionedTriggeredJob) triggeredJob, action);
        } catch(Throwable e) {
            logger.warn("JobCompleter threw unhandled execption", e);
        }
    }

    @Override
    public void shutdown() {
        jobCompleter.shutdown();
    }

    private <T> T executeInTransaction(TransactionCallback<T> callback) {
        try {
            return transactionOperations.execute(callback);
        } catch(TransactionException e) {
            throw new JobPersistenceException(e);
        } catch(DataAccessException e) {
            throw new JobPersistenceException(e);
        }
    }

    private JDBCJob createJDBCJob(JobDetail jobDetail) {
        JDBCJob jdbcJob = new JDBCJob();
        jdbcJob.setId(jobDetail.getJobKey().getKey());
        jdbcJob.setJobState(JobState.WAITING);
        jdbcJob.setJobClass(jobClassMapper.classToString(jobDetail.getJobClass()));
        jdbcJob.setPriority(jobDetail.getPriority());
        jdbcJob.setNextExecutionTime(jobDetail.getTrigger().nextExecutionTime(new DefaultTriggerContext()).get());
        if(jobDetail.getTrigger() instanceof CronTrigger) {
            CronTrigger cronTrigger = (CronTrigger) jobDetail.getTrigger();
            jdbcJob.setCronExpression(cronTrigger.getCronExpression());
        }
        jdbcJob.setVersion(0);
        jdbcJob.setJobData(jobDataEncoderDecoder.encode(jobDetail.getJobData()));
        return jdbcJob;
    }

    private Iterable<JDBCJob> createJDBCJobs(Iterable<JobDetail> jobDetails) {
        List<JDBCJob> res = new ArrayList<JDBCJob>();
        for(JobDetail jobDetail : jobDetails) {
            res.add(createJDBCJob(jobDetail));
        }
        return res;
    }

    private JobDetail convertFromJDBCJob(JDBCJob jdbcJob) throws ClassNotFoundException {
        Trigger trigger;
        if(jdbcJob.getCronExpression() == null) {
            trigger = new SimpleTrigger(jdbcJob.getNextExecutionTime());
        } else {
            trigger = new CronTrigger(jdbcJob.getCronExpression());
        }
        return new JobDetail(new JobKey(jdbcJob.getId()), createJobClass(jdbcJob.getJobClass()), trigger,
                new JobData(jobDataEncoderDecoder.decode(jdbcJob.getJobData())), jdbcJob.getPriority()
        );
    }

    private Class<? extends Job> createJobClass(String classname) throws ClassNotFoundException {
        return jobClassMapper.stringToClass(classname);
    }

    private static class NestedException extends RuntimeException {

        private static final long serialVersionUID = 2516879252642151967L;

        public NestedException(Throwable cause) {
            super(cause);
        }
    }

}
