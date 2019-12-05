package com.hoolai.bi.flink.serialization;

import com.alibaba.fastjson.JSON;
import com.hoolai.bi.flink.pojo.RequestInfo;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 *
 *@description: 
 *@author: Ksssss(chenlin@hoolai.com)
 *@time: 2019-11-30 17:56
 * 
 */
 
public class JsonToVoSchema extends AbstractDeserializationSchema<RequestInfo> {
    @Override
    public RequestInfo deserialize(byte[] bytes)  {
        RequestInfo requestInfo = null;
        try {
            requestInfo = JSON.parseObject(bytes,RequestInfo.class);
        }catch (Exception e){
           e.printStackTrace();
        }
        return requestInfo;
    }
}
