package com.hoolai.bi.job;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-25 17:31
 */

public class SyncRealTimeData implements Runnable {
    private static final int DEFAULT_BLOCK_SIZE = 100;
    private final BlockingQueue<ConsumerRecord<String, String>> ack;
    private volatile boolean isRunning = Boolean.TRUE;
    private KafkaConsumerThreadPoolExecutor executor = new KafkaConsumerThreadPoolExecutor();

    public SyncRealTimeData() {
        this(DEFAULT_BLOCK_SIZE);
    }

    public SyncRealTimeData(int blockSize) {
        this.ack = new LinkedBlockingQueue<>(blockSize);
    }

    public void put(ConsumerRecord<String, String> record) throws InterruptedException{
        if (record == null) {
            return;
        }

        try {
            ack.put(record);
        } catch (InterruptedException e) {
            throw e;
        }catch (Exception e){
            return;
        }
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                //todo if check the redis
                //if(the redis has data)  else
                ConsumerRecord<String, String> recode = ack.take();
                if (recode == null) {
                    continue;
                }
                executor.submit(new ProcessSyncRealTimeData(recode));
            } catch (InterruptedException e) {
                isRunning = false;
            }
        }
    }

    private final class ProcessSyncRealTimeData implements Runnable {
        private final ConsumerRecord<String, String> recode;

        public ProcessSyncRealTimeData(ConsumerRecord<String, String> recode) {
            this.recode = recode;
        }

        @Override
        public void run() {
            System.out.println(recode.value());
            try {
                processData(recode);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void processData(ConsumerRecord<String, String> recode) throws InterruptedException {

    }
}
