# my-flink
## 实现并发消费kafka数据并写入hdfs。 基于flink统计实时ip热点统计情况。
    kafka配置：
    bootstrap.servers=10.2.21.65:9092
    group.id=0
    enable.auto.commit=true
    auto.commit.interval.ms=1000
    key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
    value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
    session.timeout.ms=30000
    max.poll.interval.ms=2000
## 流程

https://github.com/Ksssssssss/my-flink/blob/master/src/main/resources/%E6%B5%81%E7%A8%8B.png?raw=true
