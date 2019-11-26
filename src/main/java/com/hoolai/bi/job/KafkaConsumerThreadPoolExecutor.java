package com.hoolai.bi.job;

import java.util.concurrent.*;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-23 10:56
 */
public class KafkaConsumerThreadPoolExecutor {
    private final ThreadPoolExecutor executor;
    private static final int DEFAULT_POOL_SIZE = 6;
    private static final int DEFAULT_CAPACITY_SIZE = 100;

    public KafkaConsumerThreadPoolExecutor() {
        this(DEFAULT_POOL_SIZE,DEFAULT_CAPACITY_SIZE);
    }

    public KafkaConsumerThreadPoolExecutor(int poolSize,int capacitySize) {
        this.executor = new ThreadPoolExecutor(poolSize,poolSize,0l,TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(capacitySize));
        //调用者运行策略
        this.executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }

    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

}