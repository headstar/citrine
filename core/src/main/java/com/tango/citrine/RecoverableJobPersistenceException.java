package com.tango.citrine;

/**
 * Exception thrown when a previously failed operation might be able to succeed if the application retries the operation in a new transaction.
 */
public class RecoverableJobPersistenceException extends SchedulerException {

    private static final long serialVersionUID = 1795760001634210525L;

    public RecoverableJobPersistenceException(Throwable cause) {
        super(cause);
    }
}
