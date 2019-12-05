package com.hoolai.bi;


import com.hoolai.bi.config.KafakProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Random;

@SpringBootTest
class FlinkDemoApplicationTests {

    @Test
    void contextLoads() {
    }
    @Test
    public void readProperties(){
        System.out.println(KafakProperties.PROPERTIES.getProperty("bootstrap-servers"));
    }

    @Test
    public void random(){
//        Random random = new Random(47);
//        System.out.println(random.nextInt(10));
        System.out.println(Math.max(Long.MIN_VALUE,1575461716000l));
    }

}
