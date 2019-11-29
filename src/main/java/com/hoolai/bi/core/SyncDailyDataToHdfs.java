package com.hoolai.bi.core;

import com.hoolai.bi.hdfs.HdfsWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-25 17:31
 */

@Component
public class SyncDailyDataToHdfs implements Runnable {
    private static final int DEFAULT_BLOCK_SIZE = 100;
    private final BlockingQueue<ConsumerRecord<String, String>> ack;
    private volatile boolean isRunning = Boolean.TRUE;

    private final KafkaConsumerThreadPoolExecutor executor = new KafkaConsumerThreadPoolExecutor();
    private final ExecutorService singleExecutorService = Executors.newSingleThreadExecutor();

    @Autowired
    private HdfsWriter writer;

    public SyncDailyDataToHdfs() {
        this(DEFAULT_BLOCK_SIZE);
    }

    public SyncDailyDataToHdfs(int blockSize) {
        this.ack = new LinkedBlockingQueue<>(blockSize);
    }

    public void put(ConsumerRecord<String, String> record) throws InterruptedException {
        if (record == null) {
            return;
        }

        if (!isRunning) {
            throw new IllegalStateException();
        }
        ack.put(record);
    }

    public void start() {
        singleExecutorService.submit(this);
    }

    @Override
    public void run() {
        while (isRunning) {
            //todo if check the redis
            //if(the redis has data)  else
            try {
                ConsumerRecord<String, String> recode = null;
                recode = ack.take();

                if (recode == null) {
                    continue;
                }
                executor.submit(new ProcessSyncRealTimeData(recode));
            } catch (InterruptedException e) {
                //todo retry
                continue;
            } catch (RejectedExecutionException e) {
                break;
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
            // todo save the hdfs

            try {
                writer.appendDataToHdfs(recode.value());
            } catch (Exception e) {
                isRunning = Boolean.FALSE;
                e.printStackTrace();
            } finally {
                if (Thread.currentThread().isInterrupted() && !Thread.currentThread().isAlive()) {
                    executor.add(this);
                }
            }
        }

    }

    public void stop() {
        try {
            executor.stop();
            if (!singleExecutorService.isShutdown())
                //防止阻塞队列阻塞
                singleExecutorService.shutdownNow();
        } finally {
            writer.close();
        }
    }
}
