package com.hoolai.bi.flink.operator;

import com.hoolai.bi.flink.task.LogTask;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.awt.TimedWindowEvent;

/**
 *
 *@description:
 *@author: Ksssss(chenlin@hoolai.com)
 *@time: 2019-12-04 11:40
 *
 */

public class WindowResultFunction extends ProcessWindowFunction<Long, LogTask.IpViewCount,Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context context, Iterable<Long> iterable, Collector<LogTask.IpViewCount> collector) throws Exception {
        long value = iterable.iterator().next();
        String sKey =(String)((Tuple1)key).f0;
        collector.collect(LogTask.IpViewCount.of(sKey,context.window().getEnd(),value));
    }
}
