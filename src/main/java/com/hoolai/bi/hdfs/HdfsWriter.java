package com.hoolai.bi.hdfs;

import com.hoolai.bi.config.HdfsConfig;
import org.apache.commons.io.FileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-27 10:06
 */

@Component
public class HdfsWriter {

    private String url;
    private String filePath;
    private Path path;
    private Configuration configuration;
    private FileSystem fs;
    private Lock lock = new ReentrantLock();
    private volatile boolean flag = Boolean.TRUE;

    @Autowired
    private HdfsConfig hdfsConfig;

    public HdfsWriter() {
        configuration = new Configuration();
    }

    @PostConstruct
    private void init() {
        url = hdfsConfig.getUrl();
        filePath = hdfsConfig.getPath();
        path = new Path(url + filePath);
        System.setProperty("HADOOP_USER_NAME", hdfsConfig.getUserName());

        try {
            fs = FileSystem.get(URI.create(url), configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendDataToHdfs(String datas) throws Exception{
        FSDataOutputStream fsDataOutputStream = null;

        try {
            lock.lock();
            if (!flag){
                return;
            }

            if (!fs.exists(path)) {
                fsDataOutputStream = fs.create(path);
            } else {
                fsDataOutputStream = fs.append(path);
            }
            fsDataOutputStream.write(datas.getBytes("UTF-8"));
            fsDataOutputStream.close();
        }catch (Exception e){
            flag = Boolean.FALSE;
            throw e;
        }
        finally {
            lock.unlock();
        }
    }

    public synchronized void close(){
        try {
            if (fs !=null){
                fs.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            fs = null;
        }
    }

    public static void main(String[] args) {
    }
}
