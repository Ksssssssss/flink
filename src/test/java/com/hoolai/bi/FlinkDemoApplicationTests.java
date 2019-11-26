package com.hoolai.bi;


import com.hoolai.bi.config.KafakProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest
class FlinkDemoApplicationTests {

    @Test
    void contextLoads() {
    }
    @Test
    public void readProperties(){
        System.out.println(KafakProperties.PROPERTIES.getProperty("bootstrap-servers"));
    }

}
