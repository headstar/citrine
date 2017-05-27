package com.tango.citrine.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import({ JobStoreConfigurationSelector.class, Configuration.class })
public @interface EnableCitrine {

    JobStoreType jobStoreType() default JobStoreType.JDBC;

    DatabaseType databaseType() default DatabaseType.MYSQL;

    String tableName() default "job";

    String dataSource() default "dataSource";

    String transactionManager() default "transactionManager";

}
