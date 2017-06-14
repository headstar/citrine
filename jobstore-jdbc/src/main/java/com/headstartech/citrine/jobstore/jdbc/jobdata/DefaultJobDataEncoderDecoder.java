package com.headstartech.citrine.jobstore.jdbc.jobdata;

import com.headstartech.citrine.JobData;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Created by per on 01/12/15.
 */
public class DefaultJobDataEncoderDecoder implements JobDataEncoderDecoder {

    private static final String UTF_8 = "UTF-8";

    @Override
    public String encode(JobData jobData) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : jobData.entrySet()) {
            if (!first) {
                builder.append("&");
            } else {
                first = false;
            }
            try {
                builder.append(URLEncoder.encode(entry.getKey(), UTF_8));
                builder.append("=");
                if (entry.getValue() != null) {
                    builder.append(URLEncoder.encode(entry.getValue(), UTF_8));
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return builder.toString();
    }

    @Override
    public JobData decode(String encodedJobData) {
        JobData res = new JobData();
        if (!encodedJobData.isEmpty()) {
            String[] pairs = encodedJobData.split("&");
            for (String keyValuePair : pairs) {
                String[] keyValueArr = keyValuePair.split("=");
                try {
                    String key = URLDecoder.decode(keyValueArr[0], UTF_8);
                    String value = null;
                    if (keyValueArr.length == 2) {
                        value = URLDecoder.decode(keyValueArr[1], UTF_8);
                    }
                    res.put(key, value);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return res;
    }
}
