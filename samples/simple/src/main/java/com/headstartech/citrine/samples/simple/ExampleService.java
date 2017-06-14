package com.headstartech.citrine.samples.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ExampleService {

    private static Logger logger = LoggerFactory.getLogger(ExampleService.class);

    public void log(String msg) {
        logger.info(msg);
    }
}
