package com.mycompany.springboot.spark.streaming.springbootsparkstreaming.post.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

@Component
public class AutowiredLogPostProcessor implements BeanPostProcessor {

    private Logger log = LoggerFactory.getLogger(AutowiredLogPostProcessor.class);

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithFields(bean.getClass(), field -> {
            if (field.getAnnotation(AutowiredLog.class) != null) {
                ReflectionUtils.makeAccessible(field);
                Logger logger = LoggerFactory.getLogger(bean.getClass());
                field.set(bean, logger);
                log.info("Injected logger into bean '{}'", beanName);
            }
        });
        return bean;
    }

}
