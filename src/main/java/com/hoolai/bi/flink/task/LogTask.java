package com.hoolai.bi.flink.task;

import com.hoolai.bi.config.KafakProperties;
import com.hoolai.bi.flink.operator.CountAgg;
import com.hoolai.bi.flink.operator.WindowResultFunction;
import com.hoolai.bi.flink.pojo.RequestInfo;
import com.hoolai.bi.flink.serialization.JsonToVoSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: Ksssss(chenlin @ hoolai.com)
 * @time: 2019-11-26 16:33
 */

public class LogTask {
    private static final Logger logger = LoggerFactory.getLogger("LogTask");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(2000);
        environment.setParallelism(1);

        FlinkKafkaConsumer<RequestInfo> flinkKafkaConsumer = new FlinkKafkaConsumer<>("game-report", new JsonToVoSchema(), KafakProperties.PROPERTIES);
        //默认consumer消费位置
        flinkKafkaConsumer.setStartFromLatest();
        //flink消费并提交
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        //每五分钟拿出前半个小时内每个ip的点击次数并拿出点击前三的ip点击信息
        environment.addSource(flinkKafkaConsumer).assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.of(5000, TimeUnit.MILLISECONDS)))
                .filter(new FilterFunction<RequestInfo>() {
                    @Override
                    public boolean filter(RequestInfo info) throws Exception {
                        return info.getStatus().equals("200");
                    }
                }).keyBy("xForwarded")
                .timeWindow(Time.minutes(30),Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopHotIps(3)).setParallelism(2)
                .print();

        environment.execute();
    }

    public static class IpViewCount {
        public String ip;
        public long count;
        public long windowEnd;

        public static IpViewCount of(String ip, long windowEnd, long count) {
            IpViewCount ipViewCount = new IpViewCount();
            ipViewCount.ip = ip;
            ipViewCount.count = count;
            ipViewCount.windowEnd = windowEnd;
            return ipViewCount;
        }

        @Override
        public String toString() {
            return "IpViewCount{" +
                    "ip='" + ip + '\'' +
                    ", count=" + count +
                    ", windowEnd=" + windowEnd +
                    '}';
        }
    }

    public static class TopHotIps extends KeyedProcessFunction<Tuple, IpViewCount, String> {

        private final int topSize;
        private ListState<IpViewCount> states;

        public TopHotIps(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void processElement(IpViewCount ipViewCount, Context context, Collector<String> collector) throws Exception {
            states.add(ipViewCount);
            //当前窗口结束
            context.timerService().registerEventTimeTimer(ipViewCount.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<IpViewCount> ipViewCounts = new ArrayList<>();
            for (IpViewCount ipViewCount : states.get()) {
                ipViewCounts.add(ipViewCount);
            }

            states.clear();
            ipViewCounts.sort((IpViewCount o1, IpViewCount o2) -> (int) (o2.count - o1.count));

            int suitSize = Math.min(topSize, ipViewCounts.size());
            StringBuilder sb = new StringBuilder();
            sb.append("------------------start-----------------------");
            for (int i = 0; i < suitSize; i++) {
                sb.append('\n');
                IpViewCount ipViewCount = ipViewCounts.get(i);
                sb.append("当前ip：").append(ipViewCount.ip)
                        .append("====")
                        .append("当前点击次数: ")
                        .append(ipViewCount.count)
                        .append("====")
                        .append("当前窗口结束时间: ")
                        .append(ipViewCount.windowEnd);
                sb.append('\n');
            }
            sb.append("'-------------------end------------------------");
            out.collect(sb.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //注册状态
            ListStateDescriptor<IpViewCount> stateDescriptor = new ListStateDescriptor<IpViewCount>("ip_view_state", IpViewCount.class);
            states = getRuntimeContext().getListState(stateDescriptor);
        }
    }
}
