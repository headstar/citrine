package com.headstartech.citrine.spring;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Created by per on 12/01/16.
 */
@Configuration
public class JobStoreConfigurationSelector implements ImportSelector {

    private static final String DEFAULT_JOB_STORE_TYPE_ATTRIBUTE_NAME = "jobStoreType";

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(importingClassMetadata.getAnnotationAttributes(EnableCitrine.class.getName(), false));
        if (attributes == null) {
            throw new IllegalArgumentException(String.format(
                    "@%s is not present on importing class '%s' as expected",
                    JobStoreConfigurationSelector.class.getSimpleName(), importingClassMetadata.getClassName()));
        }

        JobStoreType jobStoreType = attributes.getEnum(DEFAULT_JOB_STORE_TYPE_ATTRIBUTE_NAME);
        String[] imports = getImports(jobStoreType);
        if (imports == null) {
            throw new IllegalArgumentException(String.format("Unknown JobStoreType: '%s'", jobStoreType));
        }
        return imports;
    }

    private String[] getImports(JobStoreType jobStoreType) {
        switch(jobStoreType) {
            case JDBC:
                return new String[] { JDBCConfiguration.class.getName() };
            default:
                return null;
        }
    }
}
