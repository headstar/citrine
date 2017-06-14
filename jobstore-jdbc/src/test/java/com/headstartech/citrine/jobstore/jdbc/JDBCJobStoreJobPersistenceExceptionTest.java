package com.headstartech.citrine.jobstore.jdbc;

import com.headstartech.citrine.*;
import com.headstartech.citrine.jobstore.JobStore;
import com.headstartech.citrine.jobstore.TestJobClass1;
import com.headstartech.citrine.jobstore.jdbc.dao.JobStoreDAO;
import com.headstartech.citrine.jobstore.jdbc.jobclassmapper.DefaultJobClassMapper;
import com.headstartech.citrine.jobstore.jdbc.jobcompleter.DefaultJobCompleter;
import com.headstartech.citrine.jobstore.jdbc.jobdata.DefaultJobDataEncoderDecoder;
import org.junit.Test;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.retry.support.RetryTemplate;

import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by per on 11/01/16.
 */
public class JDBCJobStoreJobPersistenceExceptionTest {

    @Test
    public void addJob() {
        RuntimeException[] exceptions = { new RecoverableDataAccessException("foo"), new DeadlockLoserDataAccessException("foo", new RuntimeException())};

        for(RuntimeException e : exceptions) {
            // given
            JobStore jobStore = createJobStoreThrowingException(e);

            // when
            try {
                jobStore.addJob(new JobDetail(new JobKey("1"), TestJobClass1.class, new SimpleTrigger(new Date()), new JobData()), true);
                fail("should have thrown exception");
            } catch(RuntimeException re) {
                // then ..exception should be thrown
                assertTrue(re instanceof JobPersistenceException);
                assertTrue(re.getCause().equals(e));
            }
        }
    }

    @Test
    public void removeJob() {
        RuntimeException[] exceptions = { new RecoverableDataAccessException("foo"), new DeadlockLoserDataAccessException("foo", new RuntimeException())};

        for(RuntimeException e : exceptions) {
            // given
            JobStore jobStore = createJobStoreThrowingException(e);

            // when
            try {
                jobStore.removeJob(new JobKey("1"));
                fail("should have thrown exception");
            } catch(RuntimeException re) {
                // then ..exception should be thrown
                assertTrue(re instanceof JobPersistenceException);
                assertTrue(re.getCause().equals(e));
            }
        }
    }

    @Test
    public void exists() {
        RuntimeException[] exceptions = { new RecoverableDataAccessException("foo"), new DeadlockLoserDataAccessException("foo", new RuntimeException())};

        for(RuntimeException e : exceptions) {
            // given
            JobStore jobStore = createJobStoreThrowingException(e);

            // when
            try {
                jobStore.exists(new JobKey("1"));
                fail("should have thrown exception");
            } catch(RuntimeException re) {
                // then ..exception should be thrown
                assertTrue(re instanceof JobPersistenceException);
                assertTrue(re.getCause().equals(e));
            }
        }
    }

    @Test
    public void getJob() {
        RuntimeException[] exceptions = {new RecoverableDataAccessException("foo"), new DeadlockLoserDataAccessException("foo", new RuntimeException())};

        for (RuntimeException e : exceptions) {
            // given
            JobStore jobStore = createJobStoreThrowingException(e);

            // when
            try {
                jobStore.getJob(new JobKey("1"));
                fail("should have thrown exception");
            } catch (RuntimeException re) {
                // then ..exception should be thrown
                assertTrue(re instanceof JobPersistenceException);
                assertTrue(re.getCause().equals(e));
            }
        }
    }

    private JobStore createJobStoreThrowingException(RuntimeException e) {
        TestUtils.ExceptionThrowingTransactionOperations transactionOperations = new TestUtils.ExceptionThrowingTransactionOperations(e);
        JobStoreDAO dao = new TestUtils.DummyDAO();
        return new JDBCJobStore(transactionOperations, dao, new DefaultJobClassMapper(), new DefaultJobDataEncoderDecoder(),
                new DefaultJobCompleter(dao, transactionOperations, new RetryTemplate()));
    }

}
