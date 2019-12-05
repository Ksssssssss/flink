package com.hoolai.bi.flink.pojo;

import java.util.Random;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-30 17:46
 */

public class RequestInfo {
    private String timestamp;
    private String remoteAddr;
    private String referer;
    private String request;
    private String status;
    private String bytes;
    private String agent;
    private String xForwarded;
    private String upAddr;
    private String upHost;
    private String upRespTime;
    private String requerstTime;
    private int count;

    public RequestInfo() {
        Random random = new Random(47);
        count = random.nextInt(10);
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public String getxForwarded() {
        return xForwarded;
    }

    public void setxForwarded(String xForwarded) {
        this.xForwarded = xForwarded;
    }

    public String getUpAddr() {
        return upAddr;
    }

    public void setUpAddr(String upAddr) {
        this.upAddr = upAddr;
    }

    public String getUpHost() {
        return upHost;
    }

    public void setUpHost(String upHost) {
        this.upHost = upHost;
    }

    public String getUpRespTime() {
        return upRespTime;
    }

    public void setUpRespTime(String upRespTime) {
        this.upRespTime = upRespTime;
    }

    public String getRequerstTime() {
        return requerstTime;
    }

    public void setRequerstTime(String requerstTime) {
        this.requerstTime = requerstTime;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "RequestInfo{" +
                "timestamp='" + timestamp + '\'' +
                ", remoteAddr='" + remoteAddr + '\'' +
                ", referer='" + referer + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", bytes='" + bytes + '\'' +
                ", agent='" + agent + '\'' +
                ", xForwarded='" + xForwarded + '\'' +
                ", upAddr='" + upAddr + '\'' +
                ", upHost='" + upHost + '\'' +
                ", upRespTime='" + upRespTime + '\'' +
                ", requerstTime=" + requerstTime +
                '}';
    }
}
