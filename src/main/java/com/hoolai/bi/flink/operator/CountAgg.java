package com.hoolai.bi.flink.operator;

import com.hoolai.bi.flink.pojo.RequestInfo;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-12-04 10:54
 */

public class CountAgg implements AggregateFunction<RequestInfo, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0l;
    }

    @Override
    public Long add(RequestInfo requestInfo, Long aLong) {
        return aLong+1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}
