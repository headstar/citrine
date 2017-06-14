package com.headstartech.citrine;

/**
 * Thrown when trying to schedule a job and a job with the specified <code>JobKey</code> already exists.
 *
 */
public class JobAlreadyExistsException extends SchedulerException {

    private static final long serialVersionUID = 7817352270971926998L;

    public JobAlreadyExistsException(String message) {
        super(message);
    }

}
