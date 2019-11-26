package com.hoolai.bi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 *@description: 
 *@author: Ksssss(chenlin@hoolai.com)
 *@time: 2019-11-26 11:21
 * 
 */

public class KafakProperties {
    public static final Properties PROPERTIES = new Properties();

    static {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka.properties");
        try {
            PROPERTIES.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
