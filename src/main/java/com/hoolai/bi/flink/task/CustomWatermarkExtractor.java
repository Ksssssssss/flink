package com.hoolai.bi.flink.task;

import com.hoolai.bi.flink.pojo.RequestInfo;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-12-03 21:30
 */

public class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<RequestInfo> {

    private static final long serialVersionUID = -1L;
    private DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);
    private long lastEmittedWatermark = Long.MIN_VALUE;
    private long currentMaxTimestamp;
    private final long maxOutOfOrderness;

    //不知道为撒，不像这样写就无效果
    public CustomWatermarkExtractor(Time maxOutOfOrderness) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("to set maximum allowed timeout failed" + maxOutOfOrderness +
                    "This parameter is illegal");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness.toMilliseconds();
    }

    @Nullable
    @Override
    public final Watermark getCurrentWatermark() {
        // this guarantees that the watermark never goes backwards.
        long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(RequestInfo info, long previousElementTimestamp) {
        long timestamp = 0l;
        try {
            timestamp = dateFormat.parse(info.getTimestamp()).getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
