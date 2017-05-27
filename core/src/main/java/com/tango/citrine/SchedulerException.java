package com.tango.citrine;

/**
 * Base class for exceptions thrown by the <code>JobScheduler</code>.
 *
 */
public class SchedulerException extends RuntimeException {

    private static final long serialVersionUID = -7590508723135764478L;

    public SchedulerException() {
    }

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchedulerException(Throwable cause) {
        super(cause);
    }


}
