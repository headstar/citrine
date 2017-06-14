package com.headstartech.citrine.spring;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Created by per on 12/01/16.
 */
@Configuration
public abstract class AbstractJobStoreConfiguration implements ImportAware, ApplicationContextAware {

    protected AnnotationAttributes enableCitrine;
    protected ApplicationContext applicationContext;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setImportMetadata(AnnotationMetadata importMetadata) {
        enableCitrine = AnnotationAttributes.fromMap(importMetadata.getAnnotationAttributes(EnableCitrine.class.getName(), false));
        if (enableCitrine == null) {
            throw new IllegalArgumentException("@EnableCitrine is not present on importing class " + importMetadata.getClassName());
        }
    }

    protected <T> T getRequiredBeanFromAttribute(String attributeName, Class<T> clazz) {
        String beanName = enableCitrine.getString(attributeName);
        if(beanName == null) {
            throw new IllegalArgumentException(String.format("No %s specified", attributeName));
        }
        Object bean = applicationContext.getBean(beanName);
        return clazz.cast(bean);
    }

    protected <T> T getRequiredBeanFromAttribute(String attributeName, Class<T> clazz, T defaultValue) {
        String beanName = enableCitrine.getString(attributeName);
        if(beanName == null) {
            return defaultValue;
        }
        Object bean = applicationContext.getBean(beanName);
        return clazz.cast(bean);
    }
}
