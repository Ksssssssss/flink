package com.hoolai.bi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-26 12:00
 */
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaConsumerConfig {

}
