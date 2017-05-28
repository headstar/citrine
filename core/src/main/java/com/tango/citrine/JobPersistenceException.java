package com.tango.citrine;

/**
 * Exception thrown when a persistence operation failed.
 */
public class JobPersistenceException extends SchedulerException {

    private static final long serialVersionUID = 1795760001634210525L;

    public JobPersistenceException(Throwable cause) {
        super(cause);
    }
}
