package com.hoolai.bi.kafka;

import com.hoolai.bi.config.KafakProperties;
import com.hoolai.bi.core.SyncDailyDataToHdfs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-23 10:56
 */
//@Component
public class DailyKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger("consumer");
    private final String topic = "game-report";
    private final KafkaConsumer<String, String> kafkaConsumer;

    @Autowired
    private SyncDailyDataToHdfs syncDailyDataToHdfs;

    public DailyKafkaConsumer() {
        kafkaConsumer = new KafkaConsumer<String, String>(KafakProperties.PROPERTIES);
        kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @PostConstruct
    public void init() {
        syncDailyDataToHdfs.start();
        receive();
    }

    public void receive() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> recordList = new ArrayList<>(records.records(topicPartition));

                    Iterator<ConsumerRecord<String, String>> it = recordList.iterator();
                    while (it.hasNext()) {
                        ConsumerRecord<String, String> record = it.next();
                        long startTime = System.currentTimeMillis();
                        long lastOffset = recordList.get(recordList.size() - 1).offset();
                        try {
                            kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)));
                        } catch (Exception e) {
                            LOGGER.error("kafka is timeout since maybe business code processing to low,topicName:{},currentName:{},commit time:{},value{},error:{}", topic, Thread.currentThread().getName(), (System.currentTimeMillis() - startTime), record.value(), e);
                            break;
                        }

                        try {
                            syncDailyDataToHdfs.put(record);
                        } catch (IllegalArgumentException e) {
                            //todo save the remain recodeList
                            throw e;
                        }
                        it.remove();
                    }
                }
            }
        } catch (Exception e) {
            // todo
            e.printStackTrace();
        } finally {
            syncDailyDataToHdfs.stop();
            kafkaConsumer.close();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        DailyKafkaConsumer consumer = new DailyKafkaConsumer();
        consumer.receive();
        Thread.sleep(1000000);
    }
}
