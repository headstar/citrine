package com.tango.citrine.jobstore.jdbc.jobdata;

import com.tango.citrine.JobData;

/**
 * Created by per on 01/12/15.
 */
public interface JobDataEncoderDecoder {

    String encode(JobData jobData);
    JobData decode(String encodedJobData);
}
